#!/usr/bin/env bash

die() {
    echo $1
    exit 1 
}

! [[ -x $(command -v cqlsh) ]] && die "cqlsh must be installed"

SCHEMA_DIR=$(pwd)/tests/cql

[[ ! -d $SCHEMA_DIR ]] && die "Cannot locate create scripts"

echo "Running schema creation scripts..."
find $SCHEMA_DIR -name 'create_*.cql' -exec cat {} \; | cqlsh –cqlversion=3.4.4
echo "Done."