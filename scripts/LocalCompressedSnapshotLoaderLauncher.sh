#!/bin/bash
#
# ./LocalCompressedSnapshotLoaderLauncher.sh coordinatorLocator

# Script parameters
coordLoc=$1

# Edit these parameters as necessary
localSnapshotDir=/local/rcbackup/snapshot
reportFormat="OFDT"
reportInterval=2
serverSpan=80

# Directory of SnapshotLoader.
pushd `dirname $0`/.. > /dev/null                                               
snapshotLoaderDir=`pwd`                                                                   
popd > /dev/null                                                                

# Servers from which to load snapshot parts.
i=0
for j in {01..80}
do
  hosts[i]=rc$j
  (( i++ ))
done

# Create a new window with the appropriate number of panes
tmux new-window -n LocalSnapshotLoader
for (( i=0; i<${#hosts[@]}-1; i++ ))
do
  tmux split-window -h
  tmux select-layout tiled > /dev/null
done

# Setup the panes for loading but stop before executing TableUploader
for (( i=0; i<${#hosts[@]}; i++ ))
do
  # Extract tableName from the file name
  tmux send-keys -t LocalSnapshotLoader.$i "ssh ${hosts[i]}" C-m
  tmux send-keys -t LocalSnapshotLoader.$i "cd $snapshotLoaderDir" C-m
  tmux send-keys -t LocalSnapshotLoader.$i "time for file in \$(ls $localSnapshotDir); do tableName=\${file%.img.*.gz}; gunzip -c ${localSnapshotDir}/\$file | ./SnapshotLoader -C $coordLoc --tableName \$tableName --serverSpan $serverSpan --reportInterval $reportInterval --reportFormat $reportFormat; done" C-m
done
