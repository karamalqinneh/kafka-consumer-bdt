#!/bin/sh

clean=false

while getopts ':c:h:' opt; do
    case $opt in
        c) clean=true;;
        h) echo "Usage: ./hive-dump.sh [-c 1 | -h 1]"; exit;;
        *) echo "Error unknown arg!"; exit 1;;
    esac
done

echo "Configuring Hive ..."

if [ "$clean" = true ]; then
    if [ $(hive -e "show tables like 'retail_data'" | grep 'retail_data' | wc -l) -gt 0 ]; then
        echo "Deleting retail_data table"
        hive -e "drop table retail_data"
        hive -f hive.hql
    else
        echo "Clean else"
        hive -f hive.hql
    fi
else
    echo "Else"
    hive -f hive.hql
fi
