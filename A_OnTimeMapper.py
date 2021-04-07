"""
Task A: Avg On Time Arrival - Mapper

To run via Hadoop:
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -file A_OnTimeMapper.py -mapper A_OnTimeMapper.py -file A_OnTimeReducer.py -reducer A_OnTimeReducer.py -input JAN2021.csv -output a_out
cat a_out/part-00000 | more | sort -nk2

To run via cat (check only):
cat JAN2021.csv | ./A_OnTimeMapper.py | sort -k1,1 | ./A_OnTimeReducer.py | sort -nk2
"""

#!/usr/bin/env python3

import sys
import csv

# input comes from STDIN (standard input)
# for line in sys.stdin:
for row in csv.reader(iter(sys.stdin.readline, ''), delimiter=',', quotechar='"'):
    # Not: row is already in a list of strings

    # Unpack columns into variables
    (FL_DATE, OP_UNIQUE_CARRIER, OP_CARRIER_FL_NUM, ORIGIN_AIRPORT_ID, ORIGIN,
     ORIGIN_CITY_NAME, ORIGIN_STATE_NM, DEST_AIRPORT_ID, DEST, DEST_CITY_NAME,
     DEST_STATE_NM, DEP_DELAY_NEW, ARR_DELAY_NEW, CANCELLED, CANCELLATION_CODE,
     CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY,
     LATE_AIRCRAFT_DELAY, empty) = row

    # Add placeholder value for empty columns
    if not ARR_DELAY_NEW:
        ARR_DELAY_NEW = "0"

    # write the results to STDOUT (standard output);
    # what we output here will be the input for the
    # Reduce step, i.e. the input for reducer.py
    #
    # tab-delimited
    print("{0}\t{1}".format(OP_UNIQUE_CARRIER, ARR_DELAY_NEW))
