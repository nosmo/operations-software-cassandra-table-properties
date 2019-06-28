#!/usr/bin/env bash

die() {
    echo $1
    exit 1 
}

! [[ -x $(command -v cqlsh) ]] && die "cqlsh must be installed"
! [[ -x $(command -v nodetool) ]] && die "nodetool must be installed"

echo "Enumerating namespaces..."
keyspaces=$(echo desc keyspaces | cqlsh | xargs -n1 echo | grep -v ^system)
for ks in $keyspaces; do
    echo "Dropping '$ks'..."
    echo "drop keyspace \"$ks\";" | cqlsh  –cqlversion=3.4.4
done

echo "Running cleanup.."
nodetool cleanup

echo "Done."