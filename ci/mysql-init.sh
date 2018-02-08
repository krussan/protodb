#!/bin/bash
mysql -u root -e "CREATE USER protodb IDENTIFIED BY 'protodb'"
mysql -u root -e "CREATE DATABASE protodb"
mysql -u root -e "CREATE DATABASE protodb_select"
mysql -u root -e "GRANT ALL PRIVILEGES ON protodb.* TO protodb"
mysql -u root -e "GRANT ALL PRIVILEGES ON protodb_select.* TO protodb"
mysql -u root -e "GRANT FILE ON *.* TO protodb"
chmod 666 $TRAVIS_BUILD_DIR/ci/mysql/*