#!/bin/bash
# -------------------------------------------------------------------------------
# Filename:    bigdata_column_check.sh
# Revision:     1.0
# Date:          2018/02/28
# Author:      zhangtb
# Description: 大数据处理字段校验
# Notes:        需要各个文件目录具有写权限 
# -------------------------------------------------------------------------------
echo "**********门诊就诊记录和住院就诊记录表增量导入hbase start**********"
echo "传入参数的个数是$#"
echo "传入的参数为：$@"
time1=`date +%s`;
. /home/hadoop/linux_path.sh

/home/hadoop/jdk/jdk1.8.0_191/bin/java -Ddts.concurrent_num=6 -Ddts.conf_dir=/home/hadoop/dts/shell/new/new_step7 -jar /home/hadoop/jar/hbase-dts.jar

time2=`date +%s`; 
diftime=$((($time2 - $time1)/60)) ; 
echo "**********字段校验上传脚本执行结束,总共花时：$diftime 分钟！**********"
