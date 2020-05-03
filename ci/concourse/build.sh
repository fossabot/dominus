#!/usr/bin/env bash

git clone https://git@bitbucket.org/openaristos/openaristos-java-sdk.git
cd openaristos-java-sdk
mvn install

cd ../dominus
mvn package
