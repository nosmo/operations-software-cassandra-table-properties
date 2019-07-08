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

CQLSH="cqlsh"

print "Enumerating namespaces..."
keyspaces=$(echo desc keyspaces | $CQLSH | xargs -n1 echo | grep -v ^system)
for ks in $keyspaces; do
    print "Dropping '$ks'..."
    echo "drop keyspace \"$ks\";" | $CQLSH
done

print "Running cleanup.."
nodetool cleanup

print "Done."
