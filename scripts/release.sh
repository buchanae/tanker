#!/bin/bash
goreleaser --rm-dist --release-notes <(github-release-notes -org buchanae -repo tanker -since-latest-release)
