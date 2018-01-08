#!/bin/bash
# ./HashPartition.sh /path/to/images <tableId> <serverSpan>
# Get the absolute path of this script on the system.
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"

imageDir=$1
tableId=$2
serverSpan=$3

cd $imageDir
for file in $(ls *.img)
do
  mkdir ${file%.img}
  cat $file | $SCRIPTPATH/../ImageFileHashPartitioner --tableId $tableId --serverSpan $serverSpan --outputDir ${file%.img}
done

