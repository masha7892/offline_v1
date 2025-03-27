#!/bin/sh
echo "输入要删除的hdfs目录"
read dir
hdfs_dir="$dir"

del(){
  file_array=($(hdfs dfs -ls $hdfs_dir | awk '{print $8}'))

  echo "目录中有以下文件:"
  for file in "${file_array[@]}"
  do
  echo "$file"
  done


  echo "是否删除以下文件?y/n"
  read input
  case $input in
  y)
    echo "开始删除..."
    sleep 2
     for file in "${file_array[@]}"
      do
        echo "删除: $file"
        sudo -u hdfs hdfs dfs -rm -r "$file"
      done
      ;;
  n)
    echo "取消删除..."
    exit 0
    ;;
  *)
    echo "输入无效..."
    exit 1
    ;;
  esac

}

del