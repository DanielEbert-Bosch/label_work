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
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor

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
    with open('azcopy_dev_out.txt', 'wb') as f:
        f.write(output)
    with open('azcopy_dev_out.txt', 'rb') as f:
        output = f.read()

    qa_sia_blobstore = 'https://dypersiaqua.blob.core.windows.net/nrcs-2-pf/'
    qa_cmd = f'azcopy list --output-type=json --output-level=essential {qa_sia_blobstore}'
    qa_output = subprocess.check_output(qa_cmd, shell=True)
    with open('azcopy_qa_out.txt', 'wb') as f:
        f.write(qa_output)
    with open('azcopy_qa_out.txt', 'rb') as f:
        qa_output = f.read()

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


def send_new_tasks(sequences: list[Sequence]):
    # sequence to labeltaskcreate list
    tasks = []
    for sequence in sequences:
        # check if valid
        if sequence.checksum and sequence.sia_meas_id_path:
            tasks.append(dataclasses.asdict(
                LabelTaskCreate(fmc_id=str(sequence._id), fmc_data=json.dumps(sequence.fmc_data), measurement_checksum=sequence.checksum, sia_meas_id_path=sequence.sia_meas_id_path)
            ))
    
    print(f'Requesting add of {len(tasks)} tasks.')

    with open('req_add_tasks.json', 'w') as f:
        f.write(json.dumps(tasks))
    return

    r = requests.post(f'{REST_API_URL}/api/add_tasks', headers=REST_API_HEADERS, data=json.dumps(tasks))
    if r.status_code == 200:
        print('send new tasks successful')
        return
    
    print('send new tasks failed', r.status_code)
    print(r.text)


def send_set_labeled(labeled_tasks: list[Sequence]):
    with open('req_set_labeled.json', 'w') as f:
        f.write(json.dumps(labeled_tasks))
    return
    
    r = requests.post(f'{REST_API_URL}/api/set_labeled', headers=REST_API_HEADERS, data=json.dumps(labeled_tasks))
    if r.status_code == 200:
        print('send set labeled successful')
        return
    
    print('send set labeled failed', r.status_code)
    print(r.text)


def fmcTimeToEpoch(ts: str) -> int:
    return int(datetime.datetime.fromisoformat(ts.replace('Z', '+00:00')).timestamp())


def check_video_exists(container, checksum, meas_name, d):
    # sometimes video has bytesoup lz4 in name
    cut_meas_name = meas_name.split('.')[0]
    for name in list(set([cut_meas_name, meas_name])):
        if os.path.exists(f'{container}/{checksum}/video_output/{name}_{d}.mp4'):
            return True
    return False


def check_fmc(sequence: Sequence):
    fmc = sequence.fmc_data
    container = '/home/jovyan/data/ReadOnly/dyperexprod/nrcs-2-pf'
    # checksum = '3dbe19d68c1bc7c4d0bdc297062f4577ad60fa2920ba4d7842aef5cdf9c58893'
    realWorldCutoffEpch = fmcTimeToEpoch('2025-02-10T01:01:01.000Z')

    fmc_id = fmc['id']
    meas_name = next((mf['path'].split('/')[-1] for mf in fmc['measurementFiles'] if 'bytesoup' in mf['path']), None)
    checksum = next((mf['checksum'] for mf in fmc['measurementFiles'] if 'bytesoup' in mf['path']), None)
    if not checksum or not meas_name.startswith('1P_DE_LBXQ6155_ZEUS'): return [], [], []

    add_task, missing_processedlidar, missing_frontvideo, missing_previewvideo = [], [], [], []

    has_lidar = False
    if (
            not os.path.exists(f'{container}/{checksum}/processed_lidar') or
            len(os.listdir(f'{container}/{checksum}/processed_lidar')) < 2
    ):
        missing_processedlidar.append(fmc_id)
    else:
        has_lidar = True

    if fmcTimeToEpoch(fmc['creationDate']) <= realWorldCutoffEpch:
        if not check_video_exists(container, checksum, meas_name, 'front'):
            missing_frontvideo.append(fmc_id)
        elif has_lidar:
            add_task.append(sequence)
    else:
        # real world scene
        if not check_video_exists(container, checksum, meas_name, 'preview'):
            raw_preview_available = any(referenceFile['type'] == 'PREVIEW_VIDEO_MERGED' for referenceFile in fmc['referenceFiles'])
            if raw_preview_available:
                missing_previewvideo.append(fmc_id)
        elif has_lidar:
            if True:  # TODO enable again when preview video fixed
                add_task.append(sequence)

    return add_task, missing_processedlidar, missing_frontvideo, missing_previewvideo


def run():
    organization_name = 'nrcs-2-pf'
    fmc_token = request_fmc_token(organization_name)
    fmc_query = 'Car.licensePlate = "LBXQ6155" and Sequence.recordingDate > "2025-01-01" and ReferenceFile.type = "PCAP" and ReferenceFile.type = "JSON_METADATA"'

    fmc_sequences = get_sequences(fmc_query, organization_name, fmc_token)

    print(f'Found {len(fmc_sequences)} sequences.')

    sequences = [Sequence(seq['id'], seq) for seq in fmc_sequences]
    set_sequence_measurement_checksums(sequences)
    set_referenceFileTypes(sequences)
    set_sia_link(sequences)
    set_labeled(sequences)

    labeled_tasks = get_labeled_sequences(sequences)

    add_task = []
    missing_processedlidar = []
    missing_frontvideo = []
    missing_previewvideo = []

    # TODO: need to run on jupyterhub now
    with ProcessPoolExecutor() as executor:
        results = list(tqdm(executor.map(check_fmc, sequences), total=len(fmc_sequences)))
        for at, mpl, mfv, mpv in results:
            add_task.extend(at)
            missing_processedlidar.extend(mpl)
            missing_frontvideo.extend(mfv)
            missing_previewvideo.extend(mpv)

    # TODO: probably best to do a DB clear for measurements that arent in here on first run
    breakpoint()

    with open('valid_ids.txt', 'w') as f:
        f.write(json.dumps([str(s._id) for s in add_task]))
    
    with open('missing_data.json', 'w') as f:
        f.write(json.dumps({
            'missing_processedlidar': missing_processedlidar,
            'missing_frontvideo': missing_frontvideo,
            'missing_previewvideo': missing_previewvideo
        }))

    with open('valid_ids.txt', 'w') as f:
        f.write(json.dumps([str(s._id) for s in add_task]))
    
    send_new_tasks(add_task)
 
    send_set_labeled(labeled_tasks)

    sequence_id_to_bolf_path = get_sequence_id_to_bolf_path(sequences, organization_name)
    with open('labeltaskforce_bolfs_latest.json', 'w') as f:
        f.write(json.dumps(sequence_id_to_bolf_path))
        # TODO: automate to send to fmc


def main():
    while True:
        print('tick')
        run()
        time.sleep(60 * 5)


if __name__ == '__main__':
    raise SystemExit(main())
