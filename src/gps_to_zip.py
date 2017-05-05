from urllib.request import urlopen
import json
import time

def zipcode(latlng):
    key="Your Key"
    current_delay = 0.1  # Set the initial retry delay to 100ms.
    max_delay = 10  # Set the maximum retry delay to 10 seconds.
    url="https://maps.googleapis.com/maps/api/geocode/json?latlng=%s&result_type=postal_code&key=%s" % (latlng, key)
    
    while True:
        try:
            # Get the API response.
            response = urlopen(url).read().decode('utf8')
        except IOError:
            pass  # Fall through to the retry loop.
        else:
            # If we didn't get an IOError then parse the result.
            result = json.loads(response.replace('\\n', ''))
            if result['status'] == 'OK':
                for item in result['results'][0]['address_components']:
                    if item['types'][0] == 'postal_code':
                        return item['long_name']

            elif result['status'] == 'ZERO_RESULTS':
                return '0'

            elif result['status'] != 'UNKNOWN_ERROR':
                # Many API errors cannot be fixed by a retry, e.g. INVALID_REQUEST or
                # ZERO_RESULTS. There is no point retrying these requests.
                raise Exception(result['error_message'])

        if current_delay > max_delay:
            raise Exception('Too many retry attempts.')
        print('Waiting', current_delay, 'seconds before retrying.')
        time.sleep(current_delay)
        current_delay *= 2  # Increase the delay each time we retry.

with open('../results/sample_lat_lon.out') as f:
    content = f.readlines()
latlngs = [x.strip() for x in content]

zip_lat_lngs = []

for latlng in latlngs:
    temp_zipcode = zipcode(latlng)
    zip_lat_lngs.append([temp_zipcode, latlng])

with open('../results/zip_lat_lng.out', 'a') as f:
    for item in sorted(zip_lat_lngs):
        f.write(item[0] + ',' + item[1] + '\n')

