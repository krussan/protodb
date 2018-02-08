#!/bin/bash
DBUSER=$1
DBPWD=$2
DB=$3
SCRIPT_DIR=$4

#MYSQL_SCRIPT = "LOAD DATA INFILE '/c/media/tmp/csv/enumone.csv' INTO TABLE EnumOne COLUMNS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 LINES"

for f in $SCRIPT_DIR/*.sql; do 
  echo "Processing $f file.."; 
  mysql -u $DBUSER --password=$DBPWD -D $DB < $f
done

for f in $SCRIPT_DIR/*.csv; do 
  echo "Importing $f file.."; 
  filename=$(basename "$f")
  NAME="${filename%.*}"

  echo "LOAD DATA INFILE '$f' INTO TABLE $NAME COLUMNS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 LINES"  > mysql_temp.sql
  mysql -u $DBUSER --password=$DBPWD -D $DB < mysql_temp.sql
done



