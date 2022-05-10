#
# Build stage.
#

# First Docker build stage, maven modules required.
FROM maven:3.8.1-jdk-11 AS build
    

LABEL Author = Richard Damelio (rdamelio@us.ibm.com) and Timothy Figgins (timothy.figgins1@ibm.com)


# working directory in the container.
WORKDIR /usr/src/app

# Copy target jar under local target directory into the container WORKDIR/target directory.
COPY target ./target


# Set timezone for the container.
#ENV TZ=America/New_York
ENV TZ>=2021a-0+deb9u1


# Set time zone and remove the build target before a new build,Then compile your code and package it in the target dir as a jar file in the container directory listed below.
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    set -ex && \
    sed -i 's/http:\/\/deb.\(.*\)/https:\/\/deb.\1/g' /etc/apt/sources.list && \
    apt-get update && apt-get install -y && \
    apt-get update && apt-get install -y tzdata && \
	apt-get update && apt-get install -y apt-transport-https && \
  	apt-get update && apt-get install -y ca-certificates && \
  	apt-get update && apt-get install -y curl && \
  	apt-get update && apt-get install -y gnupg-agent && \
  	apt-get update && apt-get install -y software-properties-common && \
    ln -s /lib /lib64 && \
    apt-get update && apt-get install -y libapt-pkg5.0 && \
    apt-get update && apt-get install -y libzstd1 && \
    apt-get update && apt-get install --upgrade libssl1.1 && \
    apt-get update && apt-get install --upgrade openssl && \
    apt-get update && apt-get install zlib1g && \
    apt-get update && apt-get install gzip && \
    apt install -y bash tini libc6 libpam-modules libnss3 

# Second Docker build stage: copy compiled minimal Java distribution and app from stage 1 to a new image.
#FROM openjdk:11.0.6-stretch
FROM openjdk:11-jre-slim-buster


# Copy cert folder and files from the current local directory into the new image we just did above into /cert directory.
COPY cert ./cert


# Expose a port so the container can talk to the ftp server.
EXPOSE 8080


# Get the build target in the container so you can run it.
COPY --from=build /usr/src/app/target/Drupal-1.0.jar .


# Add argument for cloud object store endpoint
# ARG cos_endpoint=https://map-test-01.s3.us-east.cloud-object-storage.appdomain.cloud


# Use entrypoint so I can pass arguments, aka env variables on the command line.
# ENTRYPOINT ["java","-jar","Drupal-1.0.jar"]