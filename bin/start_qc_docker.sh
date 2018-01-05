#!/usr/bin/env bash
set -e

image="$1"
if [ -z "$image" ]; then
  echo "You must supply the image name as the first argument to this script!"
  exit 1
fi

local_config_dir="$(dirname "$(realpath "$0")")/../config"
docker run \
  -v "$local_config_dir":/srv/query-coordinator/config \
  -e SERVER_CONFIG="config/local-query-coordinator.conf" \
  -p 6030:6030 \
  -d -t "$image"
