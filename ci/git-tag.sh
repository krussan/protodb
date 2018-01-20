#!/bin/sh
git config --global user.email "builds@travis-ci.com"
git config --global user.name "Travis CI"


JARFILE=$(find target/ -name '*.jar' -exec basename {} \;)
VERSION=v`echo $JARFILE | sed 's/.jar//' | sed 's/[[:alpha:]|(|[:space:]|-]//g'`

echo "Version :: $VERSION"
echo "TRAVIS_BRANCH :: $TRAVIS_BRANCH"
echo "TRAVIS_PULL_REQUEST :: $TRAVIS_PULL_REQUEST"

if [[ "$TRAVIS_PULL_REQUEST" == "false" ]] && [[ "$TRAVIS_BRANCH" == "master" ]]; then
   git tag $VERSION -a -m "Version $VERSION"
   git push -q https://$GH_TOKEN@github.com/krussan/protodb --tags
fi
