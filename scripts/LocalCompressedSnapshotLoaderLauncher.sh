#!/bin/bash
#
# ./LocalCompressedSnapshotLoaderLauncher.sh coordinatorLocator

# Script parameters
coordLoc=$1

# Edit these parameters as necessary
localSnapshotDir=/local/rcbackup/snapshot
reportFormat="OFDT"
reportInterval=2
serverSpan=64

# Directory of SnapshotLoader.
pushd `dirname $0`/.. > /dev/null                                               
snapshotLoaderDir=`pwd`                                                                   
popd > /dev/null                                                                

# Servers from which to load snapshot parts.
i=0
for j in {01..64}
do
  hosts[i]=rc$j
  (( i++ ))
done

# Create a new window with the appropriate number of panes
tmux new-window -n SnapshotLoader
for (( i=0; i<${#host[@]}-1; i++ ))
do
  tmux split-window -h
  tmux select-layout tiled > /dev/null
done

# Setup the panes for loading but stop before executing TableUploader
for (( i=0; i<${#host[@]}-1; i++ ))
do
  # Extract tableName from the file name
  tmux send-keys -t SnapshotLoader.$i "ssh ${hosts[i]}" C-m
  tmux send-keys -t SnapshotLoader.$i "cd $snapshotLoaderDir" C-m
  tmux send-keys -t SnapshotLoader.$i "for file in \$(ls $localSnapshotDir); do tableName=\${file%*.img.*.gz}; gunzip -c ${localSnapshotDir}/\$file | ./SnapshotLoader -C $coordLoc --tableName \$tableName --serverSpan $serverSpan --reportInterval $reportInterval --reportFormat $reportFormat; done" C-m
done
