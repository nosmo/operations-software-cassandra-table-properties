#!/bin/bash
set -e

cd /test/
make dev
. venv/bin/activate
echo $VIRTUAL_ENV
/usr/local/bin/docker-entrypoint.sh &
sleep 20 #TODO replace with a real test for liveness
cqlsh < tests/itest/itest.cql
tmp_out=$(mktemp -d)
./table-properties.py -d > ${tmp_out}/output.yaml

./tests/itest/compare_yaml.py ${tmp_out}/output.yaml tests/itest/itest_output.yaml
