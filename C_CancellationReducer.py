#!/usr/bin/env python3
"""
Task C: Cancellation Reasons - Reducer

To run via Hadoop:
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
    -file C_CancellationMapper.py \
    -mapper C_CancellationMapper.py \
    -file C_CancellationReducer.py \
    -reducer C_CancellationReducer.py \
    -input JAN2021.csv \
    -output c_out
cat c_out/part-00000 | more

To run via cat (check only):
cat JAN2021.csv | ./C_CancellationMapper.py | sort -k1 | ./C_CancellationReducer.py
"""

from operator import itemgetter
import sys

current_code = None
current_count = 0
code = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    code, count = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: code) before it is passed to the reducer
    if current_code == code:
        current_count += count
    else:
        if current_code:
            # write result to STDOUT
            print("{0}\t{1}".format(current_code, current_count))
        current_count = count
        current_code = code

# do not forget to output the last code if needed!
if current_code == code:
    print("{0}\t{1}".format(current_code, current_count))
