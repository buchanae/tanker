package main

import "fmt"

// Build and version details
var (
	GitCommit   = "unknown"
	GitBranch   = "unknown"
	GitUpstream = "unknown"
	BuildDate   = "unknown"
	Version     = "unknown"
)

var tpl = `git commit:   %s
git branch:   %s
git upstream: %s
build date:   %s
version:      %s`

// VersionString formats a string with version details.
func VersionString() string {
	return fmt.Sprintf(tpl, GitCommit, GitBranch, GitUpstream, BuildDate, Version)
}
