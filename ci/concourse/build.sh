#!/usr/bin/env bash

git clone https://github.com/openaristos/openaristos-java-sdk.git
cd openaristos-java-sdk
mvn install

cd ../dominus
mvn package
