#!/bin/bash
#./TableUploader -C infrc:host=192.168.1.170,port=12246 --tableName test0004
#--serverSpan 16 --imageFile 20160526.ldbc_snb_sf0100_vertexTable.img
#--splitSuffixFormat ".part%04d" --numThreads 3
dataDir=$1
numClients=1
numThreads=1
coordLoc="basic+udp:host=192.168.1.179,port=12246"
#tableNames[0]="ldbc_snb_sf0100_01_vertexTable"
#tableNames[1]="ldbc_snb_sf0100_01_edgeListTable"
tableNames[0]="ldbc_snb_sf0001_01_vertexTable"
tableNames[1]="ldbc_snb_sf0001_01_edgeListTable"
serverSpan=4
masters=4
#imageFiles[0]="20160531.ldbc_snb_sf0100_vertexTable.img"
#imageFiles[1]="20160526.ldbc_snb_sf0100_edgeListTable.img"
imageFiles[0]="20160601.ldbc_snb_sf0001_vertexTable.img"
imageFiles[1]="20160601.ldbc_snb_sf0001_edgeListTable.img"
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

# Where in the rc reservation to start clients from.                            
clientsOffset=$(( 0 + masters ))  

# Setup the panes for loading but stop before executing TableUploader
for (( i=0; i<$numClients; i++ ))
do
  tmux select-pane -t $i
  tmux send-keys "ssh rc${hosts[clientsOffset + i]}" C-m
  tmux send-keys "cd $SCRIPTPATH; cd .." C-m
  for (( j=0; j<${#tableNames[*]}; j++ ))
  do
    tmux send-keys "./TableUploader -C $coordLoc --numClients $numClients --clientIndex $i --tableName ${tableNames[j]} --serverSpan $serverSpan --imageFile ${dataDir}/${imageFiles[j]} --splitSuffixFormat \"$splitSuffixFormat\" --numThreads $numThreads --reportInterval $reportInterval --reportFormat $reportFormat; "
  done
done

