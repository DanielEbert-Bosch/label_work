import json
import os

with open('checksums.txt') as f:
    checksums = json.loads(f.read())

root_path = '/home/jovyan/data/ReadOnly/dyperexprod/nrcs-2-pf'

missing_checksums = []

for checksum in checksums:
    if not os.path.exists(f'{root_path}/{checksum}/sequence_metadata.json'):
        missing_checksums.append(checksum)

breakpoint()

