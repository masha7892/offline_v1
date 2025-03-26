#!/bin/sh

sudo -u hdfs  \
sqoop import-all-tables \
-D org.apache.sqoop.splitter.allow_text_splitter=true \
--connect jdbc:mysql://cdh03:3306/gmall \
--username root \
--password  root \
--num-mappers 8 \
--compression-codec snappy  \
--fields-terminated-by "," \
--warehouse-dir /mysql_data/  \
--null-string '\\N' \
--null-non-string '\\N'