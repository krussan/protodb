#!/bin/bash
MYSQL_SCRIPT = "LOAD DATA INFILE '/c/media/tmp/csv/enumone.csv' INTO TABLE EnumOne COLUMNS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 LINES"
