#!/bin/bash
source ./scripts/release-vars.sh
goreleaser --rm-dist --snapshot
