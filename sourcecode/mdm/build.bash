#!/usr/bin/bash

#########################################################################################################
# Script name : build
# Usage:  build.bash work_space
# Author        Date       Comments
#--------------------------------------------------------------------
# Johnny       20200915     docker deploy script
#########################################################################################################
echo "stop docker container"
docker stop $(docker ps -aqf "name=mdm")

echo "delete docker container"
docker rm $(docker ps -aqf "name=mdm")

echo "build new image"
docker build -t  cmdp/mdm:latest .

echo "start mdm container"
docker run --env-file /opt/application/mdm/env/mdm.env --name mdm  -p 8086:8086 -v /opt/application/mdm:/opt/application/mdm -d cmdp/mdm:latest

echo "end build"
