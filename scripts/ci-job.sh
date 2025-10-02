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

: "${CI_JOB_NAME:?Error: CI_JOB_NAME environment variable must be set}"

case "$CI_JOB_NAME" in
  build_and_test)
    echo "Running build_and_test job..."
    # Build.
    #
    # cor install makes sure deps are uploaded to the cache server first;
    # TODO [tastolfi 2025-09-27] - Add a `cor pre-cache <options> <package_name>/<version> ...` command
    #
    cor install --clean
    cor build   --clean
    cor test    --only
    ;;
  release)
    echo "Running release job..."
    cor export --only
    cor export-pkg --only
    cor upload
    ;;
  *)
    echo "Unknown job type: $CI_JOB_NAME"
    exit 1
    ;;
esac
