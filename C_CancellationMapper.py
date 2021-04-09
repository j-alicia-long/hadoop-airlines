#!/usr/bin/env python3
"""
Task B: Worst routes of 3 worst airlines - Mapper

To run via Hadoop:
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -file B_WorstRoutesMapper.py -mapper B_WorstRoutesMapper.py -file B_WorstRoutesReducer.py -reducer B_WorstRoutesReducer.py -input subset_JAN2021.csv -output b_out
cat b_out/part-00000 | sort -k1,1 -k4,4n | more

To run via cat (check only):
cat JAN2021.csv | ./B_WorstRoutesMapper.py | sort -k1,3 | ./B_WorstRoutesReducer.py | sort -k1,1 -k4,4n
"""

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

     # Print cancellation codes for cancelled flights only
    if float(CANCELLED) == 1:
        print("{0}\t1".format(CANCELLATION_CODE))
