#!/usr/bin/python
# The script to profile api call to (opentsdb, metron) vs (opentsdb, hbase)

from datadiff import diff
import json
import requests
import sys
from statsboard import defs


def make_pair_request_and_compare_response(url_metron):
    # constuct the other url
    url_hbase = url_metron.replace('8080', '8081')

    # make request and get response
    response_metron = json.loads(requests.get(url_metron).text)
    response_hbase = json.loads(requests.get(url_hbase).text)

    # drop the unnecessary fileds
    for r in response_metron:
        r.pop('stats', None)
        r.pop('target', None)
        r.pop('curl_cmd', None)
        r.pop('metric', None)
        r.pop('source_url', None)
        r.pop('tags', None)
        r['data_points_cnt'] = len(r['datapoints'])
        for pair in r['datapoints']:
            pair[1] = int(pair[1])
    for r in response_hbase:
        r.pop('stats', None)
        r.pop('target', None)
        r.pop('curl_cmd', None)
        r.pop('metric', None)
        r.pop('source_url', None)
        r.pop('tags', None)
        r['data_points_cnt'] = len(r['datapoints'])
        for pair in r['datapoints']:
            pair[1] = int(pair[1])

    # calculate the diff
    df = diff(response_metron, response_hbase)

    # output result
    if df.diffs:
        print '[different]nurl metron: ' + url_metron
        print df
    else:
        print '[same]nurl metron: ' + url_metron


def main():
    dashboard_name = sys.argv[1]
    request_url_prefix = 'http://viz-statsboard-metron-001:8080/raw?from=-1hour&until=&width=250'
    defs.load_dashboards()
    dashboard_data = defs.DASHBOARDS.get(dashboard_name)
    if not dashboard_data:
        print 'Wrong dashboard name. E.x. core_services/pinandboardservice'

    all_request_urls = []
    for section_data in dashboard_data['homepage']:
        for metrics in section_data.values()[0]:
            request_url = request_url_prefix
            for metric in metrics['metrics']:
                request_url += ('&target=' + metric['stat'])
            all_request_urls.append(request_url)
    for url_metron in all_request_urls:
        make_pair_request_and_compare_response(url_metron)

if __name__ == '__main__':
    main()
