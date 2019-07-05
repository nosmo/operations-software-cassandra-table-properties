#!/usr/bin/env bash

die() {
    echo $1
    exit 1 
}

print() {
    [ ! $silent ] && echo $1
}

! [[ -x $(command -v cqlsh) ]] && die "cqlsh must be installed"
! [[ -x $(command -v nodetool) ]] && die "nodetool must be installed"

silent=false
[[ $# -gt 0 && $1 == "--silent" ]] && silent=true

print "Enumerating namespaces..."
keyspaces=$(echo desc keyspaces | cqlsh | xargs -n1 echo | grep -v ^system)
for ks in $keyspaces; do
    echo "Dropping '$ks'..."
    echo "drop keyspace \"$ks\";" | cqlsh
done

print "Running cleanup.."
nodetool cleanup

print "Done."
