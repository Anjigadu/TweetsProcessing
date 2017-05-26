#!/bin/bash
#Hive version of popular hash tags by tweets timestamp (Loaded data timestamps are modified to the start of shift interval where original timestamp falls)
echo "Hi, hive shell script! "
window=120000
shifts=60000
period=$((window/shifts))

#folder with scripts
cd /media/sf_Shared

#create tables
hive -f create.hql

#Load data
hive -f load.hql

# Insert 0 data
for i in 1 $period;
do
  hive -hiveconf shifts=$shifts -f insert.hql
done

#fill intermediate data
hive -hiveconf window=$window -f intermediate.hql

#fill final data
hive -f final.hql

echo -e "Please, press any key for exit \c "
read isExit
