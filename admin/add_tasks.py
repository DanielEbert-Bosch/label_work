from __future__ import annotations

import time
import datetime
import json
import subprocess
import re
from typing import Any
from collections import defaultdict
from dataclasses import dataclass
import os
from urllib.parse import quote
from dataclasses import dataclass
import dataclasses
import requests
from pprint import pprint

from fmc_api import request_fmc_token, get_sequences


# TODO: for testing, should be false later
SUPPORT_MF4 = True
DO_TESTING = os.getenv('DO_TESTING', '0') != '0'

# TODO: update url
if DO_TESTING:
    REST_API_URL = 'http://localhost:7100'
else:
    REST_API_URL = 'http://fe-c-017ev.lr.de.bosch.com:7100'

REST_API_HEADERS = {
    'accept': 'application/json',
    'Content-Type': 'application/json'
}


@dataclass
class Sequence:
    _id: str
    fmc_data: Any
    checksum: str | None = None
    sia_meas_id_path: str | None = None
    referenceFileTypes: list[str] | None = None
    isManuallyLabeled: bool | None = None
    label_date_epoch: int | None = None
    label_labeler: str | None = None
    label_bolf_path: str | None = None
    assigned_labeler: str | None = None


def set_sequence_measurement_checksums(sequences: list[Sequence]):
    for sequence in sequences:

        checksum = None
        for meas_file in sequence.fmc_data['measurementFiles']:
            if 'bytesoup' in meas_file['path']:
                checksum = meas_file['checksum']
            if SUPPORT_MF4 and meas_file['path'].endswith('.mf4'):
                checksum = meas_file['checksum']

        if checksum is not None:
            sequence.checksum = checksum


def set_referenceFileTypes(sequences: list[Sequence]):
    for sequence in sequences:
        sequence.referenceFileTypes = [i['type'] for i in sequence.fmc_data['referenceFiles']]


def set_labeled(sequences: list[Sequence]):
    # when run on blob store, think about how to speed this up
    sia_blobstore = 'https://dypersiadev.blob.core.windows.net/nrcs-2-pf/'
    cmd = f'azcopy list --output-type=json --output-level=essential {sia_blobstore}'
    output = subprocess.check_output(cmd, shell=True)

    qa_sia_blobstore = 'https://dypersiaqua.blob.core.windows.net/nrcs-2-pf/'
    qa_cmd = f'azcopy list --output-type=json --output-level=essential {qa_sia_blobstore}'
    qa_output = subprocess.check_output(qa_cmd, shell=True)

    label_paths = []
    for server_output in output, qa_output:
        for line in server_output.splitlines():
            try:
                label_paths.append(json.loads(json.loads(line)['MessageContent'])['Path'])
            except Exception:
                pass

    for sequence in sequences:
        if not sequence.checksum:
            continue

        # '06120852e9ace6ce4285dc8943c0ea362c7b843cc7bb0efa4251fc778a8fa014/processed_lidar/2025_01_14_17_06_55/06120852e9ace6ce4285dc8943c0ea362c7b843cc7bb0efa4251fc778a8fa014_ebd7rng_2025_01_14_17_06_55.json'
        pattern = re.compile(f'^{sequence.checksum}/processed_lidar/(?P<date>[^/]+)/{sequence.checksum}_(?P<labeler>[^_]+).+.json$')

        for label_path in label_paths:
            match = re.match(pattern, label_path)
            if not match:
                continue

            date_epoch = int(time.mktime(datetime.datetime.strptime(match.group('date'), '%Y_%m_%d_%H_%M_%S').timetuple()))
            if not sequence.label_date_epoch or date_epoch > sequence.label_date_epoch:
                sequence.label_date_epoch = date_epoch
                sequence.label_labeler = match.group('labeler')
                sequence.label_bolf_path = os.path.join(sia_blobstore, label_path)
            sequence.isManuallyLabeled = True


def set_sia_link(sequences: list[Sequence]):
    pattern = re.compile(r'https://(?P<blob_store>[^\.]+).blob.core.windows.net(?P<path>.*)')

    for sequence in sequences:
        for referenceFile in sequence.fmc_data['referenceFiles']:
            if referenceFile['type'] != 'ROF':
                continue
            rof_path = referenceFile['path']
            match = re.match(pattern, rof_path)
            if not match:
                print('Warning: unknown rof path', rof_path)
                continue

            path_query = quote(match.group('path'), safe='')
            sequence.sia_meas_id_path = f'%2F{match.group("blob_store")}{path_query}'


def get_sequence_id_to_bolf_path(sequences: list[Sequence], organization_name):
    ret = {}
    for sequence in sequences:
        if sequence.label_bolf_path:
            ret[sequence._id] = { 
                'path': sequence.label_bolf_path,
                'type_of_label': 'KPI_GENERATED_BOLF'
            }

    return {
        organization_name: ret
    }


def get_labeled_sequences(sequences: list[Sequence]):
    ret = []
    for sequence in sequences:
        if sequence.label_bolf_path:
            ret.append({
                'measurement_checksum': sequence.checksum,
                'label_bolf_path': sequence.label_bolf_path
            })
    
    return ret


def get_labeler_ranking(sequences: list[Sequence]):
    labelers: dict[str, int] = defaultdict(int)

    for sequence in sequences:
        if sequence.label_labeler:
            labelers[sequence.label_labeler] += 1

    return labelers


# copied from main.py
@dataclass
class LabelTaskCreate:
    fmc_id: str
    fmc_data: str
    measurement_checksum: str
    sia_meas_id_path: str


def main():
    organization_name = 'nrcs-2-pf'
    fmc_token = request_fmc_token(organization_name)
    # 'Sequence.name = "1P_DE_LBXQ6155_ZEUS_20250205_171724__HC"'
    # fmc_query = 'MeasurementFile.checksum = "06120852e9ace6ce4285dc8943c0ea362c7b843cc7bb0efa4251fc778a8fa014"'
    # fmc_query = 'Sequence.name ~ "Z_LBXO1994_C1_DEV_ARGUS_LIDAR_MTA2.0_Recording"'
    fmc_query = 'Car.licensePlate = "LBXQ6155" and Sequence.recordingDate > "2025-01-01" and ReferenceFile.type = "PCAP"'

    if not DO_TESTING:
        fmc_sequences = get_sequences(fmc_query, organization_name, fmc_token)
    else:
        with open('cache/fmc_sequences.json') as f:
            fmc_sequences = json.loads(f.read())

    print(f'Found {len(fmc_sequences)} sequences.')

    sequences = [Sequence(seq['id'], seq) for seq in fmc_sequences]
    set_sequence_measurement_checksums(sequences)
    set_referenceFileTypes(sequences)
    set_sia_link(sequences)
    set_labeled(sequences)

    sequence_id_to_bolf_path = get_sequence_id_to_bolf_path(sequences, organization_name)

    labeled_tasks = get_labeled_sequences() 
    
    # r = requests.post(f'{REST_API_URL}/api/set_labeled', headers=REST_API_HEADERS, data=json.dumps(labeled_tasks))
    # assert r.status_code == 200, r.text
    # pprint(r.json())


    # with open('labeltaskforce_bolfs_12_02_2025_14_28.json') as f:
    #     ...


    # sequence to labeltaskcreate list
    # tasks = []
    # for sequence in sequences:
    #     # check if valid
    #     if sequence.checksum and sequence.sia_meas_id_path:
    #         tasks.append(dataclasses.asdict(
    #             LabelTaskCreate(fmc_id=str(sequence._id), fmc_data=json.dumps(sequence.fmc_data), measurement_checksum=sequence.checksum, sia_meas_id_path=sequence.sia_meas_id_path)
    #         ))
    
    # print(f'Requesting add of {len(tasks)} tasks.')

    # r = requests.post(f'{REST_API_URL}/api/add_tasks', headers=REST_API_HEADERS, data=json.dumps(tasks))
    # assert r.status_code == 200, r.text
    # pprint(r.json())

    # r = requests.get(f'{REST_API_URL}/api/metrics', headers=REST_API_HEADERS)
    # assert r.status_code == 200, r.text
    # pprint(r.json())
 


if __name__ == '__main__':
    raise SystemExit(main())
