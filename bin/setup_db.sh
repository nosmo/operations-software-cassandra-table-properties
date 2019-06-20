#!/usr/bin/env bash

die() {
    echo $1
    exit 1 
}

# $(command -v cqlsh) && die "cqlsh must be installed"

CQLSH=$(which cqlsh)
PWD=$(pwd)
SCHEMA_DIR=$PWD/tests/schemas

[[ ! -d $SCHEMA_DIR/create ]] && die "Cannot locate create scripts"

# for $f in $(ls ../tests/schemas/create); {
#     echo $f
# }
cat $SCHEMA_DIR/create/excalibur.cql | cqlsh
