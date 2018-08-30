package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/alecthomas/units"
	"github.com/ncw/swift"
)

const SwiftProtocol = "swift://"

// SwiftConfig configures the OpenStack Swift object storage backend.
type SwiftConfig struct {
	Disabled   bool
	UserName   string
	Password   string
	AuthURL    string
	TenantName string
	TenantID   string
	RegionName string
	// Size of chunks to use for large object creation.
	// Defaults to 500 MB if not set or set below 10 MB.
	// The max number of chunks for a single object is 1000.
	ChunkSizeBytes int64
	// The maximum number of times to retry on error.
	// Defaults to 3.
	MaxRetries int
}

// Valid validates the SwiftConfig configuration.
func (s SwiftConfig) Valid() bool {
	user := s.UserName != "" || os.Getenv("OS_USERNAME") != ""
	password := s.Password != "" || os.Getenv("OS_PASSWORD") != ""
	authURL := s.AuthURL != "" || os.Getenv("OS_AUTH_URL") != ""
	tenantName := s.TenantName != "" || os.Getenv("OS_TENANT_NAME") != "" || os.Getenv("OS_PROJECT_NAME") != ""
	tenantID := s.TenantID != "" || os.Getenv("OS_TENANT_ID") != "" || os.Getenv("OS_PROJECT_ID") != ""
	region := s.RegionName != "" || os.Getenv("OS_REGION_NAME") != ""

	valid := user && password && authURL && tenantName && tenantID && region

	return !s.Disabled && valid
}

// Swift provides access to an sw object store.
type Swift struct {
	conn      *swift.Connection
	chunkSize int64
}

// NewSwift creates an Swift client instance, give an endpoint URL
// and a set of authentication credentials.
func NewSwift(conf SwiftConfig) (*Swift, error) {

	// Create a connection
	conn := &swift.Connection{
		UserName: conf.UserName,
		ApiKey:   conf.Password,
		AuthUrl:  conf.AuthURL,
		Tenant:   conf.TenantName,
		TenantId: conf.TenantID,
		Region:   conf.RegionName,
	}

	// Read environment variables and apply them to the Connection structure.
	// Won't overwrite any parameters which are already set in the Connection struct.
	err := conn.ApplyEnvironment()
	if err != nil {
		return nil, err
	}

	err = conn.Authenticate()
	if err != nil {
		return nil, err
	}

	var chunkSize int64
	if conf.ChunkSizeBytes < int64(100*units.MB) {
		chunkSize = int64(500 * units.MB)
	} else if conf.ChunkSizeBytes > int64(5*units.GB) {
		chunkSize = int64(5 * units.GB)
	} else {
		chunkSize = conf.ChunkSizeBytes
	}

	return &Swift{conn, chunkSize}, nil
}

// Stat returns metadata about the given url, such as checksum.
func (sw *Swift) Stat(ctx context.Context, url string) (*Object, error) {
	u, err := sw.parse(url)
	if err != nil {
		return nil, err
	}

	info, _, err := sw.conn.Object(u.bucket, u.path)
	if err != nil {
		return nil, &swiftError{"getting object info", url, err}
	}
	return &Object{
		URL:          url,
		Name:         info.Name,
		Size:         info.Bytes,
		LastModified: info.LastModified,
		ETag:         info.Hash,
	}, nil
}

// List lists the objects at the given url.
func (sw *Swift) List(ctx context.Context, url string) ([]*Object, error) {
	u, err := sw.parse(url)
	if err != nil {
		return nil, err
	}

	objs, err := sw.conn.ObjectsAll(u.bucket, &swift.ObjectsOpts{
		Prefix: u.path,
	})
	if err != nil {
		return nil, &swiftError{"listing objects by prefix", url, err}
	}

	var objects []*Object
	for _, obj := range objs {
		objects = append(objects, &Object{
			URL:          SwiftProtocol + u.bucket + "/" + obj.Name,
			Name:         obj.Name,
			Size:         obj.Bytes,
			LastModified: obj.LastModified,
			ETag:         obj.Hash,
		})
	}
	return objects, nil
}

// Get copies an object from storage to the host.
func (sw *Swift) Get(ctx context.Context, url string, dest io.Writer) (*Object, error) {
	u, err := sw.parse(url)
	if err != nil {
		return nil, err
	}

	var headers swift.Headers

	obj, err := sw.Stat(ctx, url)
	if err != nil {
		return nil, err
	}

	f, _, err := sw.conn.ObjectOpen(u.bucket, u.path, true, headers)
	if err != nil {
		return nil, &swiftError{"initiating download", url, err}
	}
	defer f.Close()

	_, copyErr := io.Copy(dest, ContextReader(ctx, f))
	if copyErr != nil {
		return nil, &swiftError{"copying file", url, copyErr}
	}

	return obj, nil
}

// Put copies an object (file) from the host to storage.
func (sw *Swift) Put(ctx context.Context, url string, src io.Reader) (*Object, error) {

	u, err := sw.parse(url)
	if err != nil {
		return nil, err
	}

	writer, err := sw.conn.StaticLargeObjectCreate(&swift.LargeObjectOpts{
		Container:  u.bucket,
		ObjectName: u.path,
		ChunkSize:  sw.chunkSize,
	})
	if err != nil {
		return nil, &swiftError{"creating object", url, err}
	}

	_, copyErr := io.Copy(writer, ContextReader(ctx, src))
	closeErr := writer.Close()
	if copyErr != nil {
		return nil, &swiftError{"copying file", url, copyErr}
	}
	if closeErr != nil {
		return nil, &swiftError{"closing upload", url, closeErr}
	}

	return sw.Stat(ctx, url)
}

// Join joins the given URL with the given subpath.
func (sw *Swift) Join(url, path string) (string, error) {
	return strings.TrimSuffix(url, "/") + "/" + path, nil
}

func (sw *Swift) parse(rawurl string) (*urlparts, error) {
	ok := strings.HasPrefix(rawurl, SwiftProtocol)
	if !ok {
		return nil, &ErrUnsupportedProtocol{"swift"}
	}

	path := strings.TrimPrefix(rawurl, SwiftProtocol)
	if path == "" {
		return nil, &ErrInvalidURL{"swift"}
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

type swiftError struct {
	msg, url string
	err      error
}

func (s *swiftError) Error() string {
	return fmt.Sprintf("swift: %s for URL %q: %v", s.msg, s.url, s.err)
}
