#!/bin/bash
echo Initiating build ...
echo Checking version ...
VERSION=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version |grep -Ev '(^\[|Download\w+:)'`

echo
echo -----------------------------------------------------
echo VERSION :: $VERSION
echo TRAVIS_PULL_REQUEST :: $TRAVIS_PULL_REQUEST
echo TRAVIS_BRANCH :: $TRAVIS_BRANCH
echo -----------------------------------------------------
echo

if [[ "$TRAVIS_BRANCH" == "master" ]];then
   echo Checking that the resulting tag does not exist

   if git rev-parse -q --verify "refs/tags/v$VERSION" >/dev/null; then
      echo ERROR! Tag $VERSION exist. Please modify pom and commit.
      exit 1
   fi
fi

if [[ "$TRAVIS_PULL_REQUEST" == "false" ]] && [[ "$TRAVIS_BRANCH" == "master" ]];then
   echo Packaging new release ...
   mvn clean package -B -DargLine="-DselectParamsFile=selectTestParams.csv -DtestParamsFile=testParams.csv"
else 
   echo Running test ...
   mvn test -B -DargLine="-DselectParamsFile=selectTestParams.csv -DtestParamsFile=testParams.csv"
fi

