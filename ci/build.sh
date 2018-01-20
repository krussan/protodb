#!/bin/bash
echo Initiating build ...

if [[ "$TRAVIS_PULL_REQUEST" == "false" ]] && [[ "$TRAVIS_BRANCH" == "master" ]];then
   echo Packaging new release ...
   mvn clean package -B
elif [[ "$TRAVIS_PULL_REQUEST" == "true" ]] && [[ "$TRAVIS_BRANCH" == "master" ]];then
   echo Pull request build.
   echo Checking that the resulting tag does not exist
else 
   echo Running test ...
   mvn test -B
fi

