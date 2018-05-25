#!/usr/bin/env bash
set -e

image="$1"
if [ -z "$image" ]; then
  echo "You must supply the image name as the first argument to this script!"
  exit 1
fi

local_config_dir="$(dirname "$(realpath "$0")")/../configs"
docker run \
  -v "$local_config_dir":/srv/query-coordinator/configs \
  -e SERVER_CONFIG="/srv/query-coordinator/configs/application.conf" \
  -p 6030:6030 \
  "$image"
