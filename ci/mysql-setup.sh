#!/bin/bash
DBUSER=$1
DBPWD=$2
DB=$3
SCRIPT_DIR=$4

#MYSQL_SCRIPT = "LOAD DATA INFILE '/c/media/tmp/csv/enumone.csv' INTO TABLE EnumOne COLUMNS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 LINES"

for f in $SCRIPT_DIR/*.sql; do 
  echo "Processing $f file.."; 
  DIRNAME=$(dirname $f)
  FILENAME=$(basename "$f")
  FULLPATH=$(cd "$DIRNAME"; pwd)/$FILENAME
  NAME="${FILENAME%.*}"

  mysql -u $DBUSER --password=$DBPWD -D $DB -e "DROP TABLE IF EXISTS $NAME"
  mysql -u $DBUSER --password=$DBPWD -D $DB < $FULLPATH
done

for f in $SCRIPT_DIR/*.csv; do 
  echo "Importing $f file.."; 
  DIRNAME=$(dirname $f)
  FILENAME=$(basename "$f")
  FULLPATH=$(cd "$DIRNAME"; pwd)/$FILENAME
  FULLPATH=$(echo $FULLPATH | sed "s/\//\\\\\//gi")
  NAME="${FILENAME%.*}"

   sed "s/__FILE__/$FULLPATH/gi" $SCRIPT_DIR/import.script | \
      sed "s/__TABLE__/$NAME/gi" | \
      mysql -u $DBUSER --password=$DBPWD -D $DB --local-infile=1
done



