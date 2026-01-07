#!/bin/bash -x
#
set -Eeuo pipefail

SCRIPT_DIR=$(realpath $(dirname "$0"))
PROJECT_DIR=$(realpath $(dirname "${SCRIPT_DIR}"))

"${SCRIPT_DIR}/ci-print-diagnostics.sh" "ci-job-with-docker.sh"

# Run the ci-job.sh script using docker.
#
ROOT_IMAGE=registry.gitlab.com/batteriesincluded/batt-docker/batteries-debian12-build-tools:0.5.0
USER_IMAGE=$(cor docker user-image ${ROOT_IMAGE} --user-commands-file="${SCRIPT_DIR}/ci-job-setup.dockerfile")

VOLUMES=
if [ -f "${HOME}/conan-local-cache-server-config.sh" ]; then
    VOLUMES+="--volume '${HOME}/conan-local-cache-server-config.sh:/conan-local-cache-server-config.sh'"
fi

docker run \
       --ulimit memlock=-1:-1 \
       --cap-add SYS_ADMIN --device /dev/fuse \
       --privileged \
       --network host \
       --env CACHE_CONAN_REMOTE \
       --env CACHE_CONAN_LOGIN_USERNAME \
       --env CACHE_CONAN_PASSWORD \
       --env RELEASE_CONAN_REMOTE \
       --env RELEASE_CONAN_LOGIN_USERNAME \
       --env RELEASE_CONAN_PASSWORD \
       --env CI_JOB_NAME \
       --volume "${HOME}/ci_conan_hosts:/etc/hosts:ro" \
       --volume "${PROJECT_DIR}:${PROJECT_DIR}" \
       ${VOLUMES} \
       --workdir "${PROJECT_DIR}" \
       ${USER_IMAGE} \
       "${BUILD_COMMAND:-${SCRIPT_DIR}/ci-job.sh}"
