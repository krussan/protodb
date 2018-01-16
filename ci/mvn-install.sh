#!/bin/sh
JARFILE=$(find target/ -name '*.jar' -exec basename {} \;)

if [[ $TRAVIS_PULL_REQUEST == “false” ]] && [[ $TRAVIS_BRANCH == “master” ]]; then
  GIT_BASE=$TRAVIS_BUILD_DIR/..
  MVN_REPO=$GIT_BASE/maven-repo

  git clone https://$GH_TOKEN@github.com/krussan/maven-repo $MVN_REPO
  
  cd $TRAVIS_BUILD_DIR/target
  $MVN_REPO/mvn-install.sh ../pom.xml $JARFILE
    
  cd $MVN_REPO
  git commit -a -m "Adding $JARFILE"
  git push

fi


