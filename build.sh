#!/usr/bin/env bash
set -e

#################################################
### Builds Flink 1.1.3 with a custom Akka version
### Please see FLINK-2821
#################################################
### Assumes to run from the Flink source root
#
# custom Akka location (may be empty)
AKKA_LOCATION="/Users/max/Dev/akka"
#
##########

mvn --version

ORIGINAL_LOCATION=$PWD

if [ "$AKKA_LOCATION" == "" ]; then

    git clone https://github.com/mxm/akka akka-custom

    cd akka-custom
else
    cd "$AKKA_LOCATION"
fi

#git checkout release-2.3-custom
#sbt clean compile

mvn install:install-file \
    -Dfile=akka-remote/target/akka-remote_2.10-2.3-SNAPSHOT.jar \
    -DgroupId=com.dataartisans.flakka \
    -DartifactId=flakka-remote_2.10 -Dversion=2.3-custom -Dpackaging=jar


cd "$ORIGINAL_LOCATION"

mvn clean install -Dmaven.javadoc.skip=true -DskipTests -Drat.skip=true
