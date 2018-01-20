#!/bin/sh
if [[ $TRAVIS_PULL_REQUEST == “false” ]] && [[ $TRAVIS_BRANCH == “master” ]]; then
   mvn clean package -B
else
   mvn test -B
fi

