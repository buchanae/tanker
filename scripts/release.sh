#!/bin/bash
notes=$(github-release-notes -org buchanae -repo tanker -since-latest-release)
goreleaser --rm-dist --release-notes $notes
