with open('../results/zip_lat_lng.out') as f:
    content = f.readlines()
zip_latlngs = [x.strip().split(',', 1) for x in content]

cleaned_zip_lat_lngs = []

MAX_NUM = 5

count = {'0':5}

for item in zip_latlngs:
    if item[0] not in count.keys():
        cleaned_zip_lat_lngs.append(item)
        count[item[0]] = 1
    elif count[item[0]] == MAX_NUM:
        continue
    elif count[item[0]] <= MAX_NUM:
        cleaned_zip_lat_lngs.append(item)
        count[item[0]] += 1

with open('../results/cleaned_zip_lat_lng.out', 'w') as f:
    for item in sorted(cleaned_zip_lat_lngs):
        f.write(item[0] + ',' + item[1] + '\n')