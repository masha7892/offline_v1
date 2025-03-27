#!/bin/sh

sudo -u hdfs  \
sqoop import-all-tables \
-D org.apache.sqoop.splitter.allow_text_splitter=true \
--connect jdbc:mysql://cdh03:3306/gmall \
--username root \
--password  root \
--num-mappers 1 \
--compression-codec snappy  \
--fields-terminated-by "," \
--warehouse-dir /hive/mysql_data/ \
--null-string '\\N' \
--null-non-string '\\N'