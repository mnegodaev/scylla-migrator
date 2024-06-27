#!/bin/bash

set -e
set -x

#workaround for number exceptions, once new sbt will be used + 2.12 scala below won't be needed
export TERM=xterm-color 

git submodule update --init --recursive

TMPDIR="$PWD"/tmpexec
mkdir -p "$TMPDIR"
trap "rm -rf $TMPDIR" EXIT
pushd spark-cassandra-connector
sbt -Djava.io.tmpdir="$TMPDIR" ++2.12.11 assembly
popd

if [ ! -d "./migrator/lib" ]; then
    mkdir migrator/lib
fi

cp ./spark-cassandra-connector/connector/target/scala-2.12/spark-cassandra-connector-assembly-*.jar ./migrator/lib

sbt -Djava.io.tmpdir="$TMPDIR" -mem 8192 ++2.12.11 migrator/assembly
