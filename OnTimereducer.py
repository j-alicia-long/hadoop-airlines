#!/usr/bin/env python3

from operator import itemgetter
import sys

curr_airline = None
curr_delay_total = 0

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    airline, delay = line.split('\t', 1)

    # convert delay (currently a string) to int
    try:
        delay = float(delay)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if curr_airline == airline:
        curr_delay_total += delay
    else: # Different airline, so restart running count
        if curr_airline:
            # write result to STDOUT
            print("{0}\t{1}".format(curr_airline, curr_delay_total))
        curr_delay_total = delay
        curr_airline = airline

# do not forget to output the last word if needed!
if curr_airline == airline:
    print("{0}\t{1}".format(curr_airline, curr_delay_total))
