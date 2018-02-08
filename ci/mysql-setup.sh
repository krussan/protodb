#!/bin/bash
DBUSER=$1
DBPWD=$2
DB=$3
SCRIPT_DIR=$4

#MYSQL_SCRIPT = "LOAD DATA INFILE '/c/media/tmp/csv/enumone.csv' INTO TABLE EnumOne COLUMNS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 LINES"

for f in $SCRIPT_DIR/*.sql; do 
  echo "Processing $f file.."; 
  FULLPATH=$(realpath $f | sed "s/\//\\\\\//gi")
  FILENAME=$(basename "$f")
  NAME="${FILENAME%.*}"

  echo "DROP TABLE $NAME" | mysql -u $DBUSER --password=$DBPWD -D $DB
  mysql -u $DBUSER --password=$DBPWD -D $DB < $f
done

for f in $SCRIPT_DIR/*.csv; do 
  echo "Importing $f file.."; 
  FULLPATH=$(realpath $f | sed "s/\//\\\\\//gi")
  FILENAME=$(basename "$f")
  NAME="${FILENAME%.*}"

   sed "s/__FILE__/$FULLPATH/gi" $SCRIPT_DIR/import.script | \
      sed "s/__TABLE__/$NAME/gi" | \
      mysql -u $DBUSER --password=$DBPWD -D $DB 
done



