#!/bin/sh
echo Initiating build ...

if [[ $TRAVIS_PULL_REQUEST == “false” ]] && [[ $TRAVIS_BRANCH == “master” ]]; then
   echo Packaging new release ...
   mvn clean package -B
else
   echo Running test ...
   mvn test -B
fi

