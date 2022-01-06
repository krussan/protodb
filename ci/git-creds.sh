#!/bin/bash
git config credential.helper "store --file=.git/credentials"
echo "https://krussan:${GH_TOKEN}@github.com" > .git/credentials