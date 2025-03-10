import json

def split_array(arr, x):
    return [arr[i:i + x] for i in range(0, len(arr), x)]

with open('missing_data.json') as f:
    missing_data = json.loads(f.read())

for name, ids in missing_data.items():
    print(name)
    for id_part in split_array(ids, 1000):
        ids_str = '","'.join([str(i) for i in id_part])
        print(f'Sequence.id in ["{ids_str}"]')

    print()
