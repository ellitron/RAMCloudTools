#!/bin/bash
mvn exec:java -Dexec.mainClass="net.ellitron.ramcloudtools.TimeOp" -Dexec.args="$*"
