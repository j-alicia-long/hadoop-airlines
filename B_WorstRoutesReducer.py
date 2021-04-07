#!/usr/bin/env python3
"""
Task B: Worst routes of 3 worst airlines - Reducer

To run via Hadoop:
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -file B_WorstRoutesMapper.py -mapper B_WorstRoutesMapper.py -file B_WorstRoutesReducer.py -reducer B_WorstRoutesReducer.py -input subset_JAN2021.csv -output b_out
cat b_out/part-00000 | sort -k1,1 -k4,4n | more

To run via cat (check only):
cat JAN2021.csv | ./B_WorstRoutesMapper.py | sort -k1,3 | ./B_WorstRoutesReducer.py | sort -k1,1 -k4,4n
"""

from operator import itemgetter
import sys

curr_route = None
curr_route_count = 0
curr_delay_total = 0

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    airline, origin, dest, delay = line.split('\t', 3)
    route = "-".join([airline, origin, dest])

    # convert delay (currently a string) to float
    try:
        delay = float(delay)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if curr_route == route:
        curr_delay_total += delay
        curr_route_count += 1
    else: # Different route, so restart running count
        if curr_route:
            # write average delay for current route to STDOUT
            airline, origin, dest = route.split('-')
            avg_route_delay = curr_delay_total/curr_route_count
            print("{0}\t{1}\t{2}\t{3}".format(airline, origin, dest, avg_route_delay))
        curr_delay_total = delay
        curr_route_count = 1
        curr_route = route

# do not forget to output the last route if needed!
if curr_route == route:
    avg_route_delay = curr_delay_total/curr_route_count
    print("{0}\t{1}\t{2}\t{3}".format(airline, origin, dest, avg_route_delay))
