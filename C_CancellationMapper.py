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

# Results from task A
WORST_AIRLINES = {"MQ", "G4", "B6"}

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

    # Print routes for worst airlines only
    if OP_UNIQUE_CARRIER not in WORST_AIRLINES:
        continue

    # write the results to STDOUT (standard output);
    # what we output here will be the input for the
    # Reduce step, i.e. the input for reducer.py
    #
    # tab-delimited
    print("{0}\t{1}\t{2}\t{3}".format(OP_UNIQUE_CARRIER, ORIGIN, DEST, ARR_DELAY_NEW))
