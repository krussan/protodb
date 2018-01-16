#!/bin/sh
mvn test -B -Dprotoc.exec=$TRAVIS_BUILD_DIR/ci/protoc

