#!/bin/zsh
#
# Pull the bus data and load it

NOW=$(date +"%F");

#Grab Datafile (statically)
echo curl --header "X-App-Token: 1qBsY5fMYDUlsqdHXN4ULAD7d" "http://data.cityofchicago.org/api/views/mq3i-nnqe/rows.csv" > BusData.csv
curl --header "X-App-Token: 1qBsY5fMYDUlsqdHXN4ULAD7d" "http://data.cityofchicago.org/api/views/mq3i-nnqe/rows.csv" > BusData.csv

#run loader Script
echo psql -f pgSql.sql -a -L$NOW-BusData.log 

psql -f ~/Documents/Code/civis/pgSql.sql -L$NOW-BusData.log --set "inFolder=/Users/danielgasperut/Documents/code/civis/"



