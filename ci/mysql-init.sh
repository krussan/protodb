#!/bin/bash
mysql -u root -e "CREATE USER protodb IDENTIFIED BY 'protodb'"
mysql -u root -e "CREATE DATABASE protodb"
mysql -u root -e "CREATE DATABASE protodb_select"
mysql -u root -e "GRANT ALL PRIVILEGES ON protodb.* TO protodb"
mysql -u root -e "GRANT ALL PRIVILEGES ON protodb_select.* TO protodb"
mysql -u root -e "GRANT FILE ON *.* TO protodb"
chmod 666 $TRAVIS_BUILD_DIR/ci/mysql/*

cat << HERE > $TRAVIS_BUILD_DIR/selectTestParams.csv
org.sqlite.JDBC;protodb_select_test.db
com.mysql.jdbc.Driver;jdbc:mysql://localhost/protodb_select?user=protodb&password=protodb&connectTimeout=1500
HERE
