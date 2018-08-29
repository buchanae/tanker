package storage

import (
	"context"
  "io"
	"time"
	"fmt"
	"strings"
	"github.com/alecthomas/units"
)

type Config struct {
	GoogleCloud GoogleCloudConfig
	Swift         SwiftConfig
	FTP    FTPConfig
}

func DefaultConfig() Config {
  return Config{
		Swift: SwiftConfig{
			MaxRetries:     20,
			ChunkSizeBytes: int64(500 * units.MB),
		},
		FTP: FTPConfig{
			Timeout:  Duration(time.Second * 10),
			User:     "anonymous",
			Password: "anonymous",
		},
  }
}

// Storage provides an interface for a storage backend,
// providing access to concrete storage systems such as Google Storage,
// local filesystem, etc.
//
// Storage backends must be safe for concurrent use.
type Storage interface {
	// Stat returns information about the object at the given storage URL.
	Stat(ctx context.Context, url string) (*Object, error)

	// List a directory. Calling List on a File is an error.
	List(ctx context.Context, url string) ([]*Object, error)

	// Get a single object from storage URL, written to a local file path.
	Get(ctx context.Context, url string, dest io.Writer) (*Object, error)

	// Put a single object to storage URL, from a local file path.
	// Returns the Object that was created in storage.
	Put(ctx context.Context, url string, src io.Reader) (*Object, error)

	// Join a directory URL with a subpath.
	Join(url, path string) (string, error)
}

// Object represents metadata about an object in storage.
type Object struct {
	// The storage-specific full URL of the object.
	// e.g. for S3 this might be "s3://my-bucket/dir1/obj.txt"
	URL string

	// The name of the object in the storage system.
	// e.g. for S3 this might be "dir/object.txt"
	Name string

	// ETag is an identifier for a specific version of the object.
	// This is an opaque string. Each system has a different representation:
	// md5, sha1, crc32, etc. This field may be empty, if the system can't provide
	// a unique ID (for example the local filesystem).
	ETag string

	LastModified time.Time

	// Size of the object, in bytes.
	Size int64
}

type urlparts struct {
	bucket, path string
}

func NewStorage(url string, conf Config) (Storage, error) {

  if strings.HasPrefix(url, GSProtocol) {
    if !conf.GoogleCloud.Valid() {
      return nil, fmt.Errorf("failed to configure Google Storage backend")
    }
    gs, err := NewGoogleCloud(conf.GoogleCloud)
    if err != nil {
      return nil, fmt.Errorf("failed to configure Google Storage backend: %s", err)
    }
    return gs, nil
  }

  if strings.HasPrefix(url, SwiftProtocol) {
    if !conf.Swift.Valid() {
      return nil, fmt.Errorf("failed to config Swift storage backend")
    }
    s, err := NewSwift(conf.Swift)
    if err != nil {
      return nil, fmt.Errorf("failed to config Swift storage backend: %s", err)
    }
    return s, nil
  }

  if strings.HasPrefix(url, FTPProtocol) {
    if !conf.FTP.Valid() {
      return nil, fmt.Errorf("failed to config ftp storage backend")
    }
    ftp, err := NewFTP(conf.FTP)
    if err != nil {
      return nil, fmt.Errorf("failed to config ftp storage backend: %s", err)
    }
    return ftp, nil
  }

  return nil, fmt.Errorf("failed to find matching storage backend for %q", url)
}

// Duration is a wrapper type for time.Duration which provides human-friendly
// text (un)marshaling.
// See https://github.com/golang/go/issues/16039
type Duration time.Duration

// String returns the string representation of the duration.
func (d *Duration) String() string {
	return time.Duration(*d).String()
}

// UnmarshalText parses text into a duration value.
func (d *Duration) UnmarshalText(text []byte) error {
	// Ignore if there is no value set.
	if len(text) == 0 {
		return nil
	}
	// Otherwise parse as a duration formatted string.
	duration, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}

	// Set duration and return.
	*d = Duration(duration)
	return nil
}

// MarshalText converts a duration to text.
func (d Duration) MarshalText() (text []byte, err error) {
	return []byte(d.String()), nil
}

// Set sets the duration from the given string.
// Implements the pflag.Value interface.
func (d *Duration) Set(raw string) error {
	return d.UnmarshalText([]byte(raw))
}

// Type returns the name of this type.
// Implements the pflag.Value interface.
func (d *Duration) Type() string {
	return "duration"
}
