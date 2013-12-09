import json
import time
from subprocess import Popen, PIPE
from itertools import groupby
from fuzzywuzzy import fuzz
from pprint import pprint
from ggeocoder import Geocoder, GeocodeError


def test():
    address = ""
    ggeocoder = Geocoder()
    for d in ggeocoder.geocode(address).data:
        pprint(dict(orig=address, result=d.data))


def main():
    limit = 3000
    with open('input.txt', 'r') as f:
        with open('output2.txt', 'w') as fw:
            for address in f:
                limit -= 1
                if limit >= 0:
                    try:
                        geocodeit(address, fw)
                    except GeocodeError as e:
                        if e.status == GeocodeError.G_GEO_OVER_QUERY_LIMIT:
                            time.sleep(2)
                        else:
                            fw.write(json.dumps(dict(orig=address, error=e.status)) + "\n")


def geocodeit(address, fw):
    ggeocoder = Geocoder()
    for d in ggeocoder.geocode(address).data:
        print json.dumps(dict(orig=address, result=d.data))
        fw.write(json.dumps(dict(orig=address, result=d.data)) + '\n')


def check_distance():
    distance_list = sorted(get_distances(), key=lambda x: float(x[0]))
    print '\n'.join(str(x) for x in distance_list)


def get_distances():
    with open('output_inter.txt', 'r') as f:
        data = [json.loads(x) for x in f.read().strip().split('\n')]

    for group, grouped in groupby(data, lambda x: x['orig']):
        grouped_list = list(grouped)
        if len(grouped_list) == 1:
            result_count = grouped_list[0]
            if 'result' not in result_count:
                continue
            result = result_count['result']['geometry']
            if 'bounds' in result and not result_count['result'].get('partial_match'):
                latNE = result['bounds']['northeast']['lat']
                lngNE = result['bounds']['northeast']['lng']
                latSW = result['bounds']['southwest']['lat']
                lngSW = result['bounds']['southwest']['lng']
                # print latNE, lngNE, latSW, lngSW
                x = Popen(["node", "test.js", str(latNE), str(lngNE), str(latSW), str(lngSW)], stdout=PIPE)
                yield x.communicate()[0].strip(), result_count['orig'], result_count['result']['formatted_address']


def analyze():
    with open('output_inter.txt', 'r') as f:
        data = [json.loads(x) for x in f.read().strip().split('\n')]

    output_set = set(x['orig'].strip() for x in data)
    print "Total Output:", len(data)
    print "Unique Output:", len(output_set)

    results = []
    for group, grouped in groupby(data, lambda x: x['orig']):
        grouped_list = list(grouped)
        if len(grouped_list) == 1:
            for grouped_item in grouped_list:
                if 'result' not in grouped_item or 'partial_match' in grouped_item['result']:
                    continue
                ratio = fuzz.ratio(group.strip().lower(), grouped_item['result']['formatted_address'].lower())
                if ratio < 70:
                    results.append((str(ratio), group.strip(), grouped_item['result']['formatted_address']))

    sorted_results = sorted(results, key=lambda x: x[0])
    for r in sorted_results:
        print '---'.join(r)


if __name__ == '__main__':
    # main()
    # analyze()
    # test()
    check_distance()
