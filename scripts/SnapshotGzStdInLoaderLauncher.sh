#!/bin/bash
#
# ./SnapshotGzStdInLoaderLauncher.sh /path/to/snapshot/ coordinatorLocator
#
# Loads the snapshot located in /path/to/snapshot into the RAMCloud cluster
# managed by the coordinator with address coordinatorLocator.

# Argument parameters
snapshotDir=$1
coordLoc=$2

# Edit these parameters as necessary
serverSpan=64
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
for j in {01..80}
do
  hosts[i]=rc$j
  (( i++ ))
done

fileList=$(ls $snapshotDir)
fileCount=0
for file in $fileList
do
  (( fileCount++ ))
done

# Create a new window with the appropriate number of panes
tmux new-window -n SnapshotLoader
for (( i=0; i<$fileCounts-1; i++ ))
do
  tmux split-window -h
  tmux select-layout tiled > /dev/null
done

# Setup the panes for loading but stop before executing TableUploader
i=0
for file in $fileList
do
  tmux send-keys -t SnapshotLoader.$i "ssh ${hosts[i]}" C-m
  tmux send-keys -t SnapshotLoader.$i "cd $snapshotLoaderDir" C-m
  tmux send-keys -t SnapshotLoader.$i "gunzip -c ${snapshotDir}/$file | ./SnapshotLoader -C $coordLoc --serverSpan $serverSpan --reportInterval $reportInterval --reportFormat $reportFormat" C-m
  (( i++ ))
done
