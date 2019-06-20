#!/usr/bin/env bash

die() {
    echo $1
    exit 1 
}

# $(command -v cqlsh) && die "cqlsh must be installed"

CQLSH=$(which cqlsh)
PWD=$(pwd)
SCHEMA_DIR=$PWD/tests/schemas

[[ ! -d $SCHEMA_DIR/drop ]] && die "Cannot locate cleanup scripts"

# for $f in $(ls ../tests/schemas/drop); {
#     echo $f
# }
cat $SCHEMA_DIR/drop/excalibur.cql | cqlsh
