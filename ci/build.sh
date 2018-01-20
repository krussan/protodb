#!/bin/bash
echo Initiating build ...
echo Checking version ...
VERSION=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version |grep -Ev '(^\[|Download\w+:)'`

echo VERSION :: $VERSION

if [[ "$TRAVIS_PULL_REQUEST" == "false" ]] && [[ "$TRAVIS_BRANCH" == "master" ]];then
   echo Packaging new release ...
   mvn clean package -B
elif [[ "$TRAVIS_PULL_REQUEST" == "true" ]] && [[ "$TRAVIS_BRANCH" == "master" ]];then
   echo Pull request build.
   echo Checking that the resulting tag does not exist
   if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
      echo ERROR! Tag $VERSION exist. Please modify pom and commit.
      exit 1
   fi
else 
   echo Running test ...
   mvn test -B
fi

