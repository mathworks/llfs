#!/bin/bash -x
#
set -Eeuo pipefail

# Pull configs for this build.
#
cor conan config install --type git https://gitlab.com/batteriesincluded/conan-config/linux-gcc12-x86_64.git

# Enable the local cache server.
#
if [ "${CACHE_CONAN_REMOTE:-}" != "" ]; then
    cor conan remote enable "${CACHE_CONAN_REMOTE}"
fi

# Select the build configuration.
#
cor select --clean --profile=linux-gcc12-x86_64 --build-type=Release

# Build.
#
# cor install makes sure deps are uploaded to the cache server first;
# TODO [tastolfi 2025-09-27] - Add a `cor pre-cache <options> <package_name>/<version> ...` command
#
cor install --clean
cor build   --clean
cor test    --only