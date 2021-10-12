#!/bin/bash
set -e

cd /test/
make dev
. venv/bin/activate

export CASSANDRA_PASSWORD="cassandra"

sed -i -e 's/AllowAllAuthenticator/PasswordAuthenticator/g' /etc/cassandra/cassandra.yaml
sed -i -e 's/AllowAllAuthorizer/CassandraAuthorizer/g' /etc/cassandra/cassandra.yaml

/usr/local/bin/docker-entrypoint.sh 2>&1 > /tmp/cassandra.log &
sleep 20 #TODO replace with a real test for liveness
cqlsh -u cassandra -p $CASSANDRA_PASSWORD < tests/itest/itest.cql
#TODO break this out into its own test with a generic script
cqlsh -u cassandra -p $CASSANDRA_PASSWORD < tests/itest/user_grants.cql

tmp_out=$(mktemp -d)
./table-properties.py -u cassandra -d -I > ${tmp_out}/output.yaml

if [ ! -f ${tmp_out}/output.yaml ]; then
    echo "Output file doesn't exist - table-properties probably failed"
    exit 1
fi

./tests/itest/compare_yaml.py ${tmp_out}/output.yaml tests/itest/itest_output.yaml

./table-properties.py -u cassandra -I tests/itest/itest_output.yaml -q && echo "Importing expected configuration matches DB state"

echo -e "\nGenerated CQL statements:"
./table-properties.py -u cassandra -I tests/itest/itest_output_alter.yaml > /tmp/generated.cql
cat /tmp/generated.cql
echo -e "\nIngesting generated CQL statements"
cqlsh -u cassandra -p $CASSANDRA_PASSWORD < /tmp/generated.cql && echo "Statements loaded successfully"
