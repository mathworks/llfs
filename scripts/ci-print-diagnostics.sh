#!/bin/bash
#
set -Eeuo pipefail

script=${1:-$0}

echo "(from ${script})"
echo "CACHE_CONAN_REMOTE='${CACHE_CONAN_REMOTE:-}'"
echo "CACHE_CONAN_LOGIN_USERNAME='${CACHE_CONAN_LOGIN_USERNAME:-}'"
if [ "${CACHE_CONAN_PASSWORD:-}" == "" ]; then
    echo "CACHE_CONAN_PASSWORD=''"
else
    echo "CACHE_CONAN_PASSWORD=(hidden)"
fi

