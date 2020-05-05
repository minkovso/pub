#!/usr/bin/env bash

# Экранируем запятые внутри кавычек
awk 'BEGIN{FS=OFS="\""} {for (i=2;i<=NF;i+=2) gsub(",", "\,", $i)} 1' crime.csv > crime_stg.csv

# Переносим обработанный файл в stg
hdfs dfs -moveFromLocal crime_stg.csv /user/hive/warehouse/stg.db/crime_stg

# Переносим исходный файл в архив
dttm=$(date +%Y%m%d%H%M%S)
mv crime.csv crime_$dttm.csv
hdfs dfs -moveFromLocal crime_$dttm.csv /user/hive/arch/crime_arch
