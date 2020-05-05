#!/usr/bin/env bash

# Экранируем запятые внутри кавычек
awk 'BEGIN{FS=OFS="\""} {for (i=2;i<=NF;i+=2) gsub(",", "\,", $i)} 1' offense_codes.csv > offense_stg.csv

# Переносим обработанный файл в stg
hdfs dfs -moveFromLocal offense_stg.csv /user/hive/warehouse/stg.db/offense_stg

# Переносим исходный файл в архив
dttm=$(date +%Y%m%d%H%M%S)
mv offense_codes.csv offense_codes_$dttm.csv
hdfs dfs -moveFromLocal offense_codes_$dttm.csv /user/hive/arch/offense_codes
