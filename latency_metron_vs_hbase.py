#!/usr/bin/python
import csv
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


def collect_stats(metron_points, hbase_points, query):
    metron_points = sorted(metron_points)
    hbase_points = sorted(hbase_points)
    metron_length = len(metron_points)
    hbase_length = len(hbase_points)

    avg_metron = sum(metron_points)/len(metron_points)
    avg_hbase = sum(hbase_points)/len(hbase_points)

    p50_metron = metron_points[metron_length / 2]
    p50_hbase = hbase_points[hbase_length / 2]

    p90_metron = metron_points[int(metron_length * .9)]
    p90_hbase = hbase_points[int(hbase_length * .9)]

    p99_metron = metron_points[int(metron_length * .99)]
    p99_hbase = hbase_points[int(hbase_length * .99)]

    max_metron = metron_points[-1]
    max_hbase = hbase_points[-1]

    return (metron_length, avg_metron, p50_metron, p90_metron, p99_metron, max_metron, hbase_length, avg_hbase, p50_hbase, p90_hbase, p99_hbase, max_hbase, query)


def main():
    cmd_fmt = "awk '/success/ {print $6, $7, $8}' %s"
    request_to_time_mapping = {}
    log_file = sys.argv[1]
    result = subprocess.Popen(cmd_fmt % log_file, shell=True, stdout=subprocess.PIPE).stdout.read()
    for line in result.split(os.linesep):
        try:
            metron_time, hbase_time, query = line.split(" ")
        except:
            continue
        metron_time = float(metron_time)
        hbase_time = float(hbase_time)
        if query not in request_to_time_mapping:
            request_to_time_mapping[query] = {}
            request_to_time_mapping[query]['metron'] = []
            request_to_time_mapping[query]['hbase'] = []
        request_to_time_mapping[query]['metron'].append(metron_time)
        request_to_time_mapping[query]['hbase'].append(hbase_time)
    metron_all_points = []
    metron_fs_points = []
    metron_pbs_points = []
    hbase_all_points = []
    hbase_fs_points = []
    hbase_pbs_points = []
    stats = [('metron:length', 'metron:avg', 'metron:p50', 'metron:p90', 'metron:p99', 'metron:max', 'hbase:length', 'hbase:avg', 'hbase:p50', 'hbase:p90', 'hbase:p99', 'hbase:max', 'query')]
    for query, time_info in request_to_time_mapping.iteritems():
        metron_points = time_info.get('metron')
        hbase_points = time_info.get('hbase')
        if not metron_points or not hbase_points:
            continue
        stats.append(collect_stats(metron_points, hbase_points, query))
        metron_all_points += metron_points
        hbase_all_points += hbase_points
        if 'followerservice' in query:
            metron_fs_points += metron_points
            hbase_fs_points += hbase_points
        elif 'pinandboardservice' in query:
            metron_pbs_points += metron_points
            hbase_pbs_points += hbase_points
    stats.append(collect_stats(metron_fs_points, hbase_fs_points, 'summary:followerservice'))
    stats.append(collect_stats(metron_pbs_points, hbase_pbs_points, 'summary:pinandboardservice'))
    stats.append(collect_stats(metron_all_points, hbase_all_points, 'summary'))
    with open('latency.csv', 'wb') as f:
        writer = csv.writer(f)
        writer.writerows(stats)


if __name__ == '__main__':
    main()

