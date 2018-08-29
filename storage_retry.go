package main

import (
	"context"
)

// StorageRetrier wraps a storage backend with logic which will retry on error,
// with a configurable backoff strategy.
type StorageRetrier struct {
	*Retrier
	Backend Storage
}

// Stat returns metadata about the given url, such as checksum.
func (r *StorageRetrier) Stat(ctx context.Context, url string) (obj *Object, err error) {
	err = r.Retry(ctx, func() error {
		obj, err = r.Backend.Stat(ctx, url)
		return err
	})
	return
}

// List lists the objects at the given url.
func (r *StorageRetrier) List(ctx context.Context, url string) (objects []*Object, err error) {
	err = r.Retry(ctx, func() error {
		objects, err = r.Backend.List(ctx, url)
		return err
	})
	return
}

// Get copies an object from S3 to the host path.
func (r *StorageRetrier) Get(ctx context.Context, url, path string) (obj *Object, err error) {
	err = r.Retry(ctx, func() error {
		obj, err = r.Backend.Get(ctx, url, path)
		return err
	})
	return
}

// Put copies an object (file) from the host path to S3.
func (r *StorageRetrier) Put(ctx context.Context, url, path string) (obj *Object, err error) {
	err = r.Retry(ctx, func() error {
		obj, err = r.Backend.Put(ctx, url, path)
		return err
	})
	return
}

// UnsupportedOperations describes which operations (Get, Put, etc) are not
// supported for the given URL.
func (r *StorageRetrier) UnsupportedOperations(url string) UnsupportedOperations {
	return r.Backend.UnsupportedOperations(url)
}

// Join joins the given URL with the given subpath.
func (r *StorageRetrier) Join(url, path string) (string, error) {
	return r.Backend.Join(url, path)
}
