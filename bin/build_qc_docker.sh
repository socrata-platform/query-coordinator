#!/usr/bin/env bash
set -e

sbt query-coordinator/assembly
jarfile=$(ls -t query-coordinator/target/scala-2.*/query-coordinator-assembly-*.jar | head -1)
cp "$jarfile" query-coordinator/docker/query-coordinator-assembly.jar
docker build --pull -t query-coordinator query-coordinator/docker
