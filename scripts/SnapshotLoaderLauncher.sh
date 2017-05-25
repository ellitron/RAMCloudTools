#!/bin/bash
#
# ./SnapshotLoaderLauncher.sh /path/to/snapshot/ coordinatorLocator [TABLESUFFIX]
#
# Loads the snapshot located in /path/to/snapshot into the RAMCloud cluster
# managed by the coordinator with address coordinatorLocator. If provided,
# TABLESUFFIX will be appended to the table names. This is used to support
# loading multiple copies of a snapshot in a single RAMCloud cluster.
#
#./TableUploader -C infrc:host=192.168.1.170,port=12246 --tableName test0004
#--serverSpan 16 --imageFile 20160526.ldbc_snb_sf0100_vertexTable.img
#--splitSuffixFormat ".part%04d" --numThreads 3

# Argument parameters
snapshotDir=$1
coordLoc=$2

if [ -n "${3:+x}" ]
then
  tableNameSuffix=$3
fi

# Edit these parameters as necessary
numClients=4
numThreads=4
serverSpan=4
reportInterval=2
reportFormat="OFDT"

# Directory of SnapshotLoader.
pushd `dirname $0`/.. > /dev/null                                               
snapshotLoaderDir=`pwd`                                                                   
popd > /dev/null                                                                

# Get full path of snapshot directory in case given directory is relative
pushd $snapshotDir > /dev/null
snapshotDir=`pwd`
popd > /dev/null

# Create an array of the client hostnames available for launching
# SnapshotLoader instances.
i=0
for j in {75..80}
do
  hosts[i]=rc$j
  (( i++ ))
done

# Create a new window with the appropriate number of panes
tmux new-window -n SnapshotLoader
for (( i=0; i<$numClients-1; i++ ))
do
  tmux split-window -h
  tmux select-layout tiled > /dev/null
done

# Setup the panes for loading but stop before executing TableUploader
for (( i=0; i<$numClients; i++ ))
do
  tmux send-keys -t SnapshotLoader.$i "ssh ${hosts[i]}" C-m
  tmux send-keys -t SnapshotLoader.$i "cd $snapshotLoaderDir" C-m
  if [ -n "${tableNameSuffix:+x}" ]
  then
    tmux send-keys -t SnapshotLoader.$i "./SnapshotLoader -C $coordLoc --numClients $numClients --clientIndex $i --serverSpan $serverSpan --snapshotDir $snapshotDir --tableNameSuffix $tableNameSuffix --numThreads $numThreads --reportInterval $reportInterval --reportFormat $reportFormat" C-m
  else
    tmux send-keys -t SnapshotLoader.$i "./SnapshotLoader -C $coordLoc --numClients $numClients --clientIndex $i --serverSpan $serverSpan --snapshotDir $snapshotDir --numThreads $numThreads --reportInterval $reportInterval --reportFormat $reportFormat" C-m
  fi
done
