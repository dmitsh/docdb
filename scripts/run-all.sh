#!/bin/bash

set -e

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"
DIR=$(dirname "$DIR")

function cleanup {
  echo "Cleaning up"
  rm -rf $datadir
  docker stop mongodb
}

echo -n "Creating data directory "
datadir=$(mktemp -d)
echo $datadir

trap cleanup EXIT

echo "Starting MongoDB container"
docker run -it --rm -v $datadir:/data/db -p 27017:27017 --name mongodb -d mongo:5.0.2

echo "Populating DB"
go run $DIR/cmd/docdb/main.go -c $DIR/cfg/mongodb.json -i $DIR/tests/dataset.json

for f in tests/q1.json tests/q2.json tests/q3.json tests/q4.json; do
  echo "Query:"
  cat $DIR/$f
  go run $DIR/cmd/docdb/main.go -c $DIR/cfg/mongodb.json -q $DIR/$f
  echo
done

echo "Done"
