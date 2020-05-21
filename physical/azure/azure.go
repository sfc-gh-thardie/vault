package azure

import (
	"context"
	"fmt"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/hashicorp/vault/sdk/physical"
	"github.com/hashicorp/vault/sdk/helper/strutil"
	"io/ioutil"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/armon/go-metrics"
	"github.com/hashicorp/errwrap"
	log "github.com/hashicorp/go-hclog"
)

const (
	// MaxBlobSize at this time
	MaxBlobSize = 1024 * 1024 * 4
	// MaxListResults is the current default value, setting explicitly
	MaxListResults = 5000
)

// AzureBackend is a physical backend that stores data
// within an Azure blob container.
type AzureBackend struct {
	container  azblob.ContainerURL
	logger     log.Logger
	permitPool *physical.PermitPool
}

// Verify AzureBackend satisfies the correct interfaces
var _ physical.Backend = (*AzureBackend)(nil)

// NewAzureBackend constructs an Azure backend using a pre-existing
// bucket. Credentials can be provided to the backend, sourced
// from the environment, AWS credential files or by IAM role.
func NewAzureBackend(conf map[string]string, logger log.Logger) (physical.Backend, error) {
	name := os.Getenv("AZURE_BLOB_CONTAINER")
	if name == "" {
		name = conf["container"]
		if name == "" {
			return nil, fmt.Errorf("'container' must be set")
		}
	}

	accountName := os.Getenv("AZURE_ACCOUNT_NAME")
	if accountName == "" {
		accountName = conf["accountName"]
		if accountName == "" {
			return nil, fmt.Errorf("'accountName' must be set")
		}
	}

	accountKey := os.Getenv("AZURE_ACCOUNT_KEY")
	if accountKey == "" {
		accountKey = conf["accountKey"]
		// accountKey can be "" if we are using MSI
	}

	environmentName := os.Getenv("AZURE_ENVIRONMENT")
	if environmentName == "" {
		environmentName = conf["environment"]
		if environmentName == "" {
			environmentName = "AzurePublicCloud"
		}
	}

	environmentUrl := os.Getenv("AZURE_ARM_ENDPOINT")
	if environmentUrl == "" {
		environmentUrl = conf["arm_endpoint"]
	}

	var environment azure.Environment
	var err error

	if environmentUrl != "" {
		environment, err = azure.EnvironmentFromURL(environmentUrl)
		if err != nil {
			errorMsg := fmt.Sprintf("failed to look up Azure environment descriptor for URL %q: {{err}}",
				environmentUrl)
			return nil, errwrap.Wrapf(errorMsg, err)
		}
	} else {
		environment, err = azure.EnvironmentFromName(environmentName)
		if err != nil {
			errorMsg := fmt.Sprintf("failed to look up Azure environment descriptor for name %q: {{err}}",
				environmentName)
			return nil, errwrap.Wrapf(errorMsg, err)
		}
	}

	azureAuth, err := auth(environment.ResourceIdentifiers.Storage)
	if err != nil {
		errorMsg := fmt.Sprintf("failed to authenticate against storage account %q: {{err}}",
			environmentName)
		return nil, errwrap.Wrapf(errorMsg, err)
	}
	client := azblob.NewTokenCredential(azureAuth.Token().AccessToken, func(credential azblob.TokenCredential) (expires time.Duration) {
		err = azureAuth.Refresh()
		if err != nil {
			logger.Error("[Error] couldn't refresh token credential")
			return 0
		}
		expireIn, err := azureAuth.Token().ExpiresIn.Int64()
		if err != nil {
			logger.Error("[Error] couldn't retrieve jwt claim for 'expiresIn' from refreshed token")
			return 0
		}
		expires = time.Duration(int(float64(expireIn) * 0.8)) * time.Second
		credential.SetToken(azureAuth.Token().AccessToken)
		logger.Info("Refreshed token. Expires in ", azureAuth.Token().ExpiresIn.Int64())
		return
	})

	p := azblob.NewPipeline(client, azblob.PipelineOptions{})

	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.%s/%s", accountName, environment.StorageEndpointSuffix, name))

	containerURL := azblob.NewContainerURL(*URL, p)
	ctx := context.Background()

	_, err = containerURL.GetProperties(ctx, azblob.LeaseAccessConditions{})
	if err != nil {
		if aerr, ok := err.(azblob.StorageError); ok {
			switch aerr.ServiceCode() {
			case azblob.ServiceCodeContainerNotFound:
			default:
				return nil, errwrap.Wrapf(fmt.Sprintf("failed to get properties for container %q: {{err}}", name), aerr)
			}
		}
		_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
		if err != nil {
			return nil, errwrap.Wrapf(fmt.Sprintf("failed to create %q container: {{err}}", name), err)
		}
	}

	maxParStr, ok := conf["max_parallel"]
	var maxParInt int
	if ok {
		maxParInt, err = strconv.Atoi(maxParStr)
		if err != nil {
			return nil, errwrap.Wrapf("failed parsing max_parallel parameter: {{err}}", err)
		}
		if logger.IsDebug() {
			logger.Debug("max_parallel set", "max_parallel", maxParInt)
		}
	}

	a := &AzureBackend{
		container:  containerURL,
		logger:     logger,
		permitPool: physical.NewPermitPool(maxParInt),
	}
	return a, nil
}

// Put is used to insert or update an entry
func (a *AzureBackend) Put(ctx context.Context, entry *physical.Entry) error {
	defer metrics.MeasureSince([]string{"azure", "put"}, time.Now())

	if len(entry.Value) >= MaxBlobSize {
		return fmt.Errorf("value is bigger than the current supported limit of 4MBytes")
	}

	blobURL := a.container.NewBlockBlobURL(entry.Key)
	_, err := azblob.UploadBufferToBlockBlob(ctx, entry.Value, blobURL, azblob.UploadToBlockBlobOptions{
		BlockSize:   MaxBlobSize,
		Parallelism: 1,
	})
	return err
}

// Get is used to fetch an entry
func (a *AzureBackend) Get(ctx context.Context, key string) (*physical.Entry, error) {
	defer metrics.MeasureSince([]string{"azure", "get"}, time.Now())

	a.permitPool.Acquire()
	defer a.permitPool.Release()

	blobURL := a.container.NewBlobURL(key)
	downloadResponse, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
	if err != nil {
		if aerr, ok := err.(azblob.StorageError); ok {
			switch aerr.ServiceCode() {
			case azblob.ServiceCodeBlobNotFound:
				return nil, nil
			default:
				return nil, errwrap.Wrapf(fmt.Sprintf("failed to download blob %q: {{err}}", key), aerr)
			}
		}
		return nil, err
	}
	reader := downloadResponse.Body(azblob.RetryReaderOptions{MaxRetryRequests: 20})
	defer reader.Close()
	data, err := ioutil.ReadAll(reader)

	ent := &physical.Entry{
		Key:   key,
		Value: data,
	}

	return ent, err
}

// Delete is used to permanently delete an entry
func (a *AzureBackend) Delete(ctx context.Context, key string) error {
	defer metrics.MeasureSince([]string{"azure", "delete"}, time.Now())

	blobURL := a.container.NewBlobURL(key)
	_, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})

	return err
}

// List is used to list all the keys under a given
// prefix, up to the next prefix.
func (a *AzureBackend) List(ctx context.Context, prefix string) ([]string, error) {
	defer metrics.MeasureSince([]string{"azure", "list"}, time.Now())

	a.permitPool.Acquire()
	defer a.permitPool.Release()

	var keys []string
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listBlob, err := a.container.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{
			Prefix: prefix,
		})
		if err != nil {
			return nil, err
		}
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			key := strings.TrimPrefix(blobInfo.Name, prefix)
			if i := strings.Index(key, "/"); i == -1 {
				// file
				keys = append(keys, key)
			} else {
				// subdirectory
				keys = strutil.AppendIfMissing(keys, key[:i+1])
			}
		}
	}

	sort.Strings(keys)
	return keys, nil
}

func auth(resource string) (*adal.ServicePrincipalToken, error) {
	msiEndpoint, err := adal.GetMSIVMEndpoint()
	if err != nil {
		return nil, err
	}
	spt, err := adal.NewServicePrincipalTokenFromMSI(msiEndpoint, resource)
	if err != nil {
		return nil, err
	}
	if err := spt.Refresh(); err != nil {
		return nil, err
	}
	token := spt.Token()
	if token.IsZero() {
		return nil, err
	}
	return spt, nil
}
