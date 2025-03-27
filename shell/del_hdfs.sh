#!/bin/sh

hdfs_dir="/mysql_data"

del(){
  file_array=($(hdfs dfs -ls $hdfs_dir | awk '{print $8}'))

  for file in "${file_array[@]}"
  do
    echo "删除: $file"
    sudo -u hdfs hdfs dfs -rm -r "$file"
  done
}

del