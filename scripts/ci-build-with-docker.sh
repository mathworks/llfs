#!/bin/bash -x
#
set -Eeuo pipefail

SCRIPT_DIR=$(realpath $(dirname "$0"))
PROJECT_DIR=$(realpath $(dirname "${SCRIPT_DIR}"))

# Run the ci-build.sh script using docker.
#
ROOT_IMAGE=registry.gitlab.com/batteriesincluded/batt-docker/batteries-debian12-build-tools:0.5.0
USER_IMAGE=$(cor docker user-image ${ROOT_IMAGE} --user-commands-file="${SCRIPT_DIR}/ci-build-setup.dockerfile")

docker run \
       --ulimit memlock=-1:-1 \
       --cap-add SYS_ADMIN --device /dev/fuse \
       --privileged \
       --network host \
       --env CACHE_CONAN_REMOTE \
       --env CACHE_CONAN_LOGIN_USERNAME \
       --env CACHE_CONAN_PASSWORD \
       --volume "${HOME}/ci_conan_hosts:/etc/hosts:ro" \
       --volume "${PROJECT_DIR}:${PROJECT_DIR}" \
       --workdir "${PROJECT_DIR}" \
       ${USER_IMAGE} \
       "${BUILD_COMMAND:-${SCRIPT_DIR}/ci-build.sh}"