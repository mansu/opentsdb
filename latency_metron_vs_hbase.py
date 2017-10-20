#!/usr/bin/python
import os
import subprocess
import sys

"""
Note:

This script analyze the log of statsboard, grep the latency on db query and aggregate information at per query type level.

how to run
./latency_metron_vs_hbase.py <statsboard_log_for_metron> <statsboard_log_for_hbase>

e.x.
./latency_metron_vs_hbase.py statsboard.log statsboard_no_cache.log
"""

def main():
    cmd_fmt = "grep 'elapsed ' %s | awk -F 'total, |INFO: |elapsed for ' '{print $3, $4}' | awk '{print $1, substr($0, index($0,$3))}'"
    request_to_time_mapping = {}
    log_metron = sys.argv[1]
    log_hbase = sys.argv[2]
    for log_file in (log_metron, log_hbase):
        result = subprocess.Popen(cmd_fmt % log_file, shell=True, stdout=subprocess.PIPE).stdout.read()
        for line in result.split(os.linesep):
            try:
                time, query = line.split(" ", 1)
            except:
                continue
            time = float(time)
            if query not in request_to_time_mapping:
                request_to_time_mapping[query] = {}
            if log_file not in request_to_time_mapping[query]:
                request_to_time_mapping[query][log_file] = []
            request_to_time_mapping[query][log_file].append(time)
    avg_time = {'metron': [], 'hbase': []}
    for query, time_info in request_to_time_mapping.iteritems():
        if not time_info.get(log_metron) or not time_info.get(log_hbase):
            continue
        avg_metron = sum(time_info[log_metron])/len(time_info[log_metron])
        avg_hbase = sum(time_info[log_hbase])/len(time_info[log_hbase])
        avg_time['metron'].append(avg_metron)
        avg_time['hbase'].append(avg_hbase)
        print '**************************************************************'
        print query
        print 'metron: ', avg_metron, 'hbase', avg_hbase
    print 'unweighted avg metron: ', sum(avg_time['metron'])/len(avg_time['metron'])
    print 'unweighted avg hbase: ', sum(avg_time['hbase'])/len(avg_time['hbase'])


if __name__ == '__main__':
    main()
