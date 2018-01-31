#!/bin/bash
set -e

# Starts the query coordinator.
REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BINDIR=$(dirname "$REALPATH")

CONFIG="${SODA_CONFIG:-"$BINDIR/../configs/application.conf"}"
JARFILE=$("$BINDIR"/build.sh "$@")

java -Djava.net.preferIPv4Stack=true -Dconfig.file="$CONFIG" -jar "$JARFILE"
