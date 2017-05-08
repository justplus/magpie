#!/bin/sh

# create artifacts using maven
cd sdk
mvn clean install
cd ../demos/producer-demo
mvn clean package
cd ../../producer
mvn clean package

cd ..
rm -fr producer-dist
mkdir producer-dist
mkdir producer-dist/plugins

cp -r producer/target/producer-*/* producer-dist/
cp demos/producer-demo/target/producer-demo-*.zip producer-dist/plugins/

# run producer
cd producer-dist
java -jar producer-*.jar
