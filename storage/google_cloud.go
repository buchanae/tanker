package storage

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/storage/v1"
)

// The gs url protocol
const GSProtocol = "gs://"

// GoogleCloudConfig describes configuration for the Google Cloud storage backend.
type GoogleCloudConfig struct {
	Disabled bool
	// If no account file is provided then storage will try to use Google Application
	// Default Credentials to authorize and authenticate the client.
	CredentialsFile string
}

// Valid validates the Config configuration.
func (g GoogleCloudConfig) Valid() bool {
	return !g.Disabled
}

// GoogleCloud provides access to an GS object store.
type GoogleCloud struct {
	svc *storage.Service
}

// NewGoogleCloud creates an GoogleCloud client instance, give an endpoint URL
// and a set of authentication credentials.
func NewGoogleCloud(conf GoogleCloudConfig) (*GoogleCloud, error) {
	ctx := context.Background()
	client := &http.Client{}

	if conf.CredentialsFile != "" {
		// Pull the client configuration (e.g. auth) from a given account file.
		// This is likely downloaded from Google Cloud manually via IAM & Admin > Service accounts.
		bytes, rerr := ioutil.ReadFile(conf.CredentialsFile)
		if rerr != nil {
			return nil, rerr
		}

		config, tserr := google.JWTConfigFromJSON(bytes, storage.CloudPlatformScope)
		if tserr != nil {
			return nil, tserr
		}
		client = config.Client(ctx)
	} else {
		// Pull the information (auth and other config) from the environment,
		// which is useful when this code is running in a Google Compute instance.
		defClient, err := google.DefaultClient(ctx, storage.CloudPlatformScope)
		if err == nil {
			client = defClient
		}
	}

	svc, cerr := storage.New(client)
	if cerr != nil {
		return nil, cerr
	}

	return &GoogleCloud{svc}, nil
}

// Stat returns information about the object at the given storage URL.
func (gs *GoogleCloud) Stat(ctx context.Context, url string) (*Object, error) {
	u, err := gs.parse(url)
	if err != nil {
		return nil, err
	}

	obj, err := gs.svc.Objects.Get(u.bucket, u.path).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("googleStorage: calling stat on object %s: %v", url, err)
	}

	modtime, _ := time.Parse(time.RFC3339, obj.Updated)
	return &Object{
		URL:          url,
		Name:         obj.Name,
		ETag:         obj.Etag,
		Size:         int64(obj.Size),
		LastModified: modtime,
	}, nil
}

// List lists the objects at the given url.
func (gs *GoogleCloud) List(ctx context.Context, url string) ([]*Object, error) {
	u, err := gs.parse(url)
	if err != nil {
		return nil, err
	}

	var objects []*Object

	err = gs.svc.Objects.List(u.bucket).Prefix(u.path).Pages(ctx,
		func(objs *storage.Objects) error {

			for _, obj := range objs.Items {
				if strings.HasSuffix(obj.Name, "/") {
					continue
				}

				modtime, _ := time.Parse(time.RFC3339, obj.Updated)

				objects = append(objects, &Object{
					URL:          GSProtocol + obj.Bucket + "/" + obj.Name,
					Name:         obj.Name,
					ETag:         obj.Etag,
					Size:         int64(obj.Size),
					LastModified: modtime,
				})
			}
			return nil
		})

	if err != nil {
		return nil, err
	}
	return objects, nil
}

// Get copies an object from GS to the host path.
func (gs *GoogleCloud) Get(ctx context.Context, url string, dest io.Writer) (*Object, error) {
	obj, err := gs.Stat(ctx, url)
	if err != nil {
		return nil, err
	}

	u, err := gs.parse(url)
	if err != nil {
		return nil, err
	}

	resp, err := gs.svc.Objects.Get(u.bucket, u.path).Context(ctx).Download()
	if err != nil {
		return nil, fmt.Errorf("googleStorage: getting object %s: %v", url, err)
	}

	_, copyErr := io.Copy(dest, ContextReader(ctx, resp.Body))

	if copyErr != nil {
		return nil, fmt.Errorf("googleStorage: copying file: %v", copyErr)
	}

	return obj, nil
}

// Put copies an object (file) from the host path to GS.
func (gs *GoogleCloud) Put(ctx context.Context, url string, src io.Reader) (*Object, error) {
	u, err := gs.parse(url)
	if err != nil {
		return nil, err
	}

	obj := &storage.Object{
		Name: u.path,
	}

	_, err = gs.svc.Objects.Insert(u.bucket, obj).Media(ContextReader(ctx, src)).Do()
	if err != nil {
		return nil, fmt.Errorf("googleStorage: uploading object %s: %v", url, err)
	}
	return gs.Stat(ctx, url)
}

// Join joins the given URL with the given subpath.
func (gs *GoogleCloud) Join(url, path string) (string, error) {
	return strings.TrimSuffix(url, "/") + "/" + path, nil
}

func (gs *GoogleCloud) parse(rawurl string) (*urlparts, error) {
	if !strings.HasPrefix(rawurl, GSProtocol) {
		return nil, &ErrUnsupportedProtocol{"googleStorage"}
	}

	path := strings.TrimPrefix(rawurl, GSProtocol)
	if path == "" {
		return nil, &ErrInvalidURL{"googleStorage"}
	}

	split := strings.SplitN(path, "/", 2)
	url := &urlparts{}
	if len(split) > 0 {
		url.bucket = split[0]
	}
	if len(split) == 2 {
		url.path = split[1]
	}
	return url, nil
}
