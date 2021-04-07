"""
Task A: Avg On Time Arrival - Reducer

To run via Hadoop:
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -file A_OnTimeMapper.py -mapper A_OnTimeMapper.py -file A_OnTimeReducer.py -reducer A_OnTimeReducer.py -input JAN2021.csv -output a_out
cat a_out/part-00000 | more | sort -nk2

To run via cat (check only):
cat JAN2021.csv | ./A_OnTimeMapper.py | sort -k1,1 | ./A_OnTimeReducer.py | sort -nk2
"""

#!/usr/bin/env python3

from operator import itemgetter
import sys

curr_airline = None
curr_airline_count = 0
curr_delay_total = 0

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    airline, delay = line.split('\t', 1)

    # convert delay (currently a string) to float
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
        curr_airline_count += 1
    else: # Different airline, so restart running count
        if curr_airline:
            # write average delay for current airline to STDOUT
            print("{0}\t{1}".format(curr_airline, curr_delay_total/curr_airline_count))
        curr_delay_total = delay
        curr_airline_count = 1
        curr_airline = airline

# do not forget to output the last airline if needed!
if curr_airline == airline:
    print("{0}\t{1}".format(curr_airline, curr_delay_total/curr_airline_count))
