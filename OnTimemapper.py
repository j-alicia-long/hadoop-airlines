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
