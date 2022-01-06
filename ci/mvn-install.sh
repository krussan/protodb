#!/bin/bash
JARFILE=`find target/ -name '*.jar' -exec basename {} \;`

if [[ "$TRAVIS_PULL_REQUEST" == "false" ]] && [[ "$TRAVIS_BRANCH" == "master" ]];then
  echo "Installing to local maven repo...."
  MVN_REPO=$GIT_BASE/maven-repo

  #git clone git@github.com:krussan/maven-repo $MVN_REPO
  git clone https://github.com/krussan/maven-repo $MVN_REPO

  cd $TRAVIS_BUILD_DIR/target
  $MVN_REPO/mvn-install.sh ../pom.xml $JARFILE
    
  cd $MVN_REPO
  git add --all
  git commit -m "Adding $JARFILE"
  git push

  echo "DONE installing local maven repo...."

fi


