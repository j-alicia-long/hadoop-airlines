#!/usr/bin/env python3
"""
Task B: Worst routes of 3 worst airlines - Reducer

To run via Hadoop:
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
    -file B_WorstRoutesMapper.py \
    -mapper B_WorstRoutesMapper.py \
    -file B_WorstRoutesReducer.py \
    -reducer B_WorstRoutesReducer.py \
    -input JAN2021.csv \
    -output b_out
cat b_out/part-00000 | sort -k1,1 -k4,4n | more
cat b_out/part-00000 | grep MQ | sort -k4n | tail -n 15
cat b_out2/part-00000 | grep G4 | sort -k4n | tail -n 15
cat b_out2/part-00000 | grep B6 | sort -k4n | tail -n 15

To run via cat (check only):
cat JAN2021.csv | ./B_WorstRoutesMapper.py | sort -k1,3 | ./B_WorstRoutesReducer.py | sort -k1,1 -k4,4n
"""

from operator import itemgetter
import sys

curr_airline = None
# Reset for each airline
route_dict = {} # Format: key=[origin]-[dest], value=(sum, count)

def print_routes(route_dict):
    for route, delay_tuple in route_dict.items():
        o, d = route.split('-')
        delay_sum, route_count = delay_tuple
        avg_route_delay = delay_sum/route_count
        # write average delay for current route to STDOUT
        print("{0}\t{1}\t{2}\t{3}".format(curr_airline, o, d, avg_route_delay))


# input comes from STDIN
# NOTE: 4 columns, assume it's sorted only by col 1
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    airline, origin, dest, delay = line.split('\t', 3)
    route = "-".join([origin, dest])

    # convert delay (currently a string) to float
    try:
        delay = float(delay)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if curr_airline != airline: # Different route, so restart running count
        if route_dict: # Print contents if full
            print_routes(route_dict)
        # Reset airline route tracking
        curr_airline = airline
        route_dict = {}
        route_dict[route] = (delay, 1)

    else:
        if route in route_dict:
            delay_sum, route_count = route_dict[route]
            route_dict[route] = (delay_sum+delay, route_count+1)
        else:
            route_dict[route] = (delay, 1)


# do not forget to output the last airline if needed!
if curr_airline:
    print_routes(route_dict)
