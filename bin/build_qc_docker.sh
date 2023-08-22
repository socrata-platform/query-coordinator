#!/usr/bin/env bash
set -e

sbt query-coordinator/assembly
jarfile="query-coordinator/target/query-coordinator-assembly.jar"
cp "$jarfile" query-coordinator/docker/query-coordinator-assembly.jar
docker build --pull -t query-coordinator query-coordinator/docker
