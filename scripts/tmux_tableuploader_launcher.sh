#!/bin/bash
#./TableUploader -C infrc:host=192.168.1.170,port=12246 --tableName test0004
#--serverSpan 16 --imageFile 20160526.ldbc_snb_sf0100_vertexTable.img
#--splitSuffixFormat ".part%04d" --numThreads 3
numClients=6
numThreads=4
coordLoc="infrc:host=192.168.1.170,port=12246"
tableName="test0001"
serverSpan=16
masters=16
imageFile="20160526.ldbc_snb_sf0100_vertexTable.img"
splitSuffixFormat=".part%04d"
reportInterval=2
reportFormat="OFDT"

SCRIPT=`readlink -f $0`
SCRIPTPATH=`dirname $SCRIPT`

# Create an array of all the hosts we have leased via rcres
i=0
for host in `rcres ls -l | grep "$(whoami)" | cut -c13-16 | grep "rc[0-9]" | cut -c3-4`;
do
  hosts[i]=$host
  (( i++ ))
done

# Create a new window with the appropriate number of panes
tmux new-window -n TableUploader
for (( i=0; i<$numClients-1; i++ ))
do
  tmux split-window -h
  tmux select-layout tiled
done

# Setup the panes for loading but stop before executing GraphLoader
for (( i=0; i<$numClients; i++ ))
do
  tmux select-pane -t $i
  tmux send-keys "ssh rc${hosts[masters+i]}" C-m
  tmux send-keys "cd $SCRIPTPATH; cd .." C-m
  tmux send-keys "./TableUploader -C $coordLoc --numClients $numClients --clientIndex $i --tableName $tableName --serverSpan $serverSpan --imageFile $imageFile --splitSuffixFormat \"$splitSuffixFormat\" --numThreads $numThreads --reportInterval $reportInterval --reportFormat $reportFormat"
done

