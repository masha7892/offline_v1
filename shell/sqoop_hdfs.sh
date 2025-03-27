#!/bin/sh


mkdir_properties(){
  echo "-D org.apache.sqoop.splitter.allow_text_splitter=true" > /shell/sqoop.properties
  chmod 777 /shell/sqoop.properties
}

if [ ! -r "/shell/sqoop.properties" ];then
mkdir_properties
else
  rm -rf /shell/sqoop.properties
  mkdir_properties
fi

sudo -u hdfs  \
sqoop import-all-tables \
--options-file /shell/sqoop.properties \
--connect jdbc:mysql://cdh03:3306/gmall \
--username root \
--password  root \
--num-mappers 1 \
--compression-codec snappy  \
--fields-terminated-by "," \
--warehouse-dir /hive/mysql_data/  \
--null-string '\\N' \
--null-non-string '\\N'



