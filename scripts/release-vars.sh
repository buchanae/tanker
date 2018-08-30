#!/bin/bash
git_head=$(git symbolic-ref -q --short HEAD)
git_url=$(git config branch.$git_head.remote)
export GIT_COMMIT=$(git rev-parse --short HEAD)
export GIT_BRANCH=$(git symbolic-ref -q --short HEAD)
export GIT_UPSTREAM=$(git remote get-url $git_url 2> /dev/null)
