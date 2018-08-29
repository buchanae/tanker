package main

import (
  "os"
)

// Config configures the OpenStack Swift object storage backend.
type Config struct {
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

// Valid validates the Config configuration.
func (s Config) Valid() bool {
	user := s.UserName != "" || os.Getenv("OS_USERNAME") != ""
	password := s.Password != "" || os.Getenv("OS_PASSWORD") != ""
	authURL := s.AuthURL != "" || os.Getenv("OS_AUTH_URL") != ""
	tenantName := s.TenantName != "" || os.Getenv("OS_TENANT_NAME") != "" || os.Getenv("OS_PROJECT_NAME") != ""
	tenantID := s.TenantID != "" || os.Getenv("OS_TENANT_ID") != "" || os.Getenv("OS_PROJECT_ID") != ""
	region := s.RegionName != "" || os.Getenv("OS_REGION_NAME") != ""

	valid := user && password && authURL && tenantName && tenantID && region

	return !s.Disabled && valid
}
