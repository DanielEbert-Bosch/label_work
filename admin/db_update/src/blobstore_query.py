from __future__ import annotations

try:
    from azure.identity import AzureCliCredential
    from azure.core.credentials import TokenCredential, AccessToken
except ModuleNotFoundError:
    os.system('pip install azure-identity')
    from azure.identity import AzureCliCredential
    from azure.core.credentials import TokenCredential, AccessToken


from requests import get
import time
import datetime
import json
import subprocess
import re
from typing import Any
from collections import defaultdict
import os
from urllib.parse import quote
from dataclasses import dataclass
import dataclasses
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor


import logging
logger = logging.getLogger(__name__)

def calc_time(string):
    global start_time

    if string == 'start':
        start_time = time.time()
    else:
        end_time = time.time()
        return  end_time - start_time


class CachedCredential(TokenCredential):
  def __init__(self, delegate: TokenCredential, logger) -> None:
    self.delegate = delegate
    self.logger = logger
    self._token : dict[str, AccessToken] = {}

  def get_token(self, scope: str, **kwargs) -> AccessToken:
    token = self._token.get(scope)
    if not token or token.expiry < time.time():
      calc_time('start')
      self._token[scope] = token = self.delegate.get_token(scope, **kwargs)
      elapsed_time = calc_time('end')
      self.logger.info(
            f'Time taken to generate token(CachedCredential) is {elapsed_time:.2f} seconds.'
        )
    else:
        self.logger.info(
            f'Valid token exists'
        )
    return token


cachedCredential = CachedCredential(AzureCliCredential(), logger)

def request_fmc_token(organization_name, stage='prod'):
    """
    Get fmc token via AzureCliCredential. (Requires az login beforehand).

    organization_name e.g. nrcs-2-pf, ford-dat-3, uss-gen-6-pf
    """
    return cachedCredential.get_token(f'api://api-data-loop-platform-{organization_name}-{stage}/.default').token


def get_sequence(sequence_id, organization_name, fmc_token):
    """
    Does get sequence Rest call for sequence_id. Returns sequence.
    """
    fmc_headers = {
        'Cache-Control': 'no-cache',
        'Authorization': f'Bearer {fmc_token}',
        'Origin': 'https://developer.bosch-data-loop.com'
    }
    url = f'https://api.azr.bosch-data-loop.com/measurement-data-processing/v3/organizations/{organization_name}/sequence/{sequence_id}'
    response = get(url, headers=fmc_headers)
    if response.status_code == 200:
        sequence = response.json()
        return sequence
    else:
        logger.error(f'Get sequence call to FMC failed. status_code: {response.status_code}, reason: {response.reason}, url: {url}')
        return None


def get_sequences(fmc_query, organization_name, fmc_token):
    """
    Does get sequences Rest call for fmc query. Returns list of sequences.
    """
    fmc_headers = {
        'Cache-Control': 'no-cache',
        'Authorization': f'Bearer {fmc_token}',
        'Origin': 'https://developer.bosch-data-loop.com'
    }

    sequences = []

    items_per_page = 1000

    is_there_more_sequences = True
    page_index = 0
    while is_there_more_sequences:
        url = f'https://api.azr.bosch-data-loop.com/measurement-data-processing/v3/organizations/{organization_name}/sequence?itemsPerPage={items_per_page}&pageIndex={page_index}&filterQuery={fmc_query}'  # noqa: E501
        response = get(url, headers=fmc_headers)
        if response.status_code == 200:
            response_sequences = response.json()
            sequences.extend(response_sequences)
            if len(response_sequences) < items_per_page:
                is_there_more_sequences = False
        else:
            logger.error(f'Get sequences call to FMC failed. status_code: {response.status_code}, reason: {response.reason}, url: {url}')
            is_there_more_sequences = False
        page_index += 1
        print(f'FMC query at {page_index=}')

    return sequences


def get_sequences_in_collection(organization_name, collection_id, fmc_token):
    """
    Does get sequences Rest call for fmc query. Returns list of sequences.
    """
    fmc_headers = {
        'Cache-Control': 'no-cache',
        'Authorization': f'Bearer {fmc_token}',
        'Origin': 'https://developer.bosch-data-loop.com'
    }

    sequences = []

    items_per_page = 1000

    is_there_more_sequences = True
    page_index = 0
    while is_there_more_sequences:
        url = f'https://api.azr.bosch-data-loop.com/measurement-data-processing/v1/organizations/{organization_name}/sequencecollection/{collection_id}/sequences?itemsPerPage={items_per_page}&pageIndex={page_index}'  # noqa: E501
        response = get(url, headers=fmc_headers)
        if response.status_code == 200:
            response_sequences = response.json()
            sequences.extend(response_sequences)
            if len(response_sequences) < items_per_page:
                is_there_more_sequences = False
        else:
            logger.error(f'Get sequences call to FMC failed. status_code: {response.status_code}, reason: {response.reason}, url: {url}')
            is_there_more_sequences = False
        page_index += 1
        print(f'FMC query at {page_index=}')

    return sequences


def fmc_query_has_reference_file_type(sequence, type: str) -> bool:
    for reference_file in sequence['referenceFiles']:
        if reference_file['type'] == type:
            return True
    return False


def fmc_query_has_bytesoup(sequence) -> bool:
    for measurement_file in sequence['measurementFiles']:
        if 'bytesoup' in measurement_file['contentType'].lower():
            return True

    return False


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

    siaqua_path = '/home/jovyan/data/ReadOnly/dypersiaqua/nrcs-2-pf/'
    siadev_path = '/home/jovyan/data/ReadOnly/dypersiadev/nrcs-2-pf/'

    siaqua_blobstore = 'https://dypersiaqua.blob.core.windows.net/nrcs-2-pf/'
    siadev_blobstore = 'https://dypersiadev.blob.core.windows.net/nrcs-2-pf/'

    label_paths = []
    for dir, blob_url in [(siadev_path, siadev_blobstore), (siaqua_path, siaqua_blobstore)]:
        output = subprocess.check_output(['find', '.', '-type', 'f'], cwd=dir).splitlines()
        for line in output:
            line = line.decode().strip()
            if not line:
                continue
            if line.startswith('./'):
                line = line[2:]
            label_paths.append((line, blob_url))

    for sequence in sequences:
        if not sequence.checksum:
            continue

        # '06120852e9ace6ce4285dc8943c0ea362c7b843cc7bb0efa4251fc778a8fa014/processed_lidar/2025_01_14_17_06_55/06120852e9ace6ce4285dc8943c0ea362c7b843cc7bb0efa4251fc778a8fa014_ebd7rng_2025_01_14_17_06_55.json'
        pattern = re.compile(f'^{sequence.checksum}/processed_lidar/(?P<date>[^/]+)/{sequence.checksum}_(?P<labeler>[^_]+).+.json$')

        for label_path, sia_url in label_paths:
            match = re.match(pattern, label_path)
            if not match:
                continue

            date_epoch = int(time.mktime(datetime.datetime.strptime(match.group('date'), '%Y_%m_%d_%H_%M_%S').timetuple()))
            if not sequence.label_date_epoch or date_epoch > sequence.label_date_epoch:
                sequence.label_date_epoch = date_epoch
                sequence.label_labeler = match.group('labeler')
                sequence.label_bolf_path = os.path.join(sia_url, label_path)
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


def send_new_tasks(sequences: list[Sequence], new_tasks_out_file: str):
    # sequence to labeltaskcreate list
    tasks = []
    for sequence in sequences:
        # check if valid
        if sequence.checksum and sequence.sia_meas_id_path:
            tasks.append(dataclasses.asdict(
                LabelTaskCreate(fmc_id=str(sequence._id), fmc_data=json.dumps(sequence.fmc_data), measurement_checksum=sequence.checksum, sia_meas_id_path=sequence.sia_meas_id_path)
            ))

    print(f'Requesting add of {len(tasks)} tasks.')

    with open(new_tasks_out_file, 'w') as f:
        f.write(json.dumps(tasks))


def fmcTimeToEpoch(ts: str) -> int:
    return int(datetime.datetime.fromisoformat(ts.replace('Z', '+00:00')).timestamp())


def check_video_exists(container, checksum, meas_name, d):
    # sometimes video has bytesoup lz4 in name
    cut_meas_name = meas_name.split('.')[0]
    for name in list({cut_meas_name, meas_name}):
        if os.path.exists(f'{container}/{checksum}/video_output/{name}_{d}.mp4'):
            return True
    return False


def check_fmc(sequence: Sequence):
    fmc = sequence.fmc_data
    container = '/home/jovyan/data/ReadOnly/dyperexprod/nrcs-2-pf'
    realWorldCutoffEpoch = fmcTimeToEpoch('2025-02-10T13:01:01.000Z')

    fmc_id = fmc['id']
    meas_name = next((mf['path'].split('/')[-1] for mf in fmc['measurementFiles'] if 'bytesoup' in mf['path']), None)
    checksum = next((mf['checksum'] for mf in fmc['measurementFiles'] if 'bytesoup' in mf['path']), None)

    add_task, missing_processedlidar, missing_frontvideo, missing_previewvideo, missing_rawpreviewvideo, missing_siametadata = [], [], [], [], [], []

    has_siametadata = os.path.exists(f'{container}/{checksum}/sequence_metadata.json')
    if not has_siametadata:
        missing_siametadata.append(fmc_id)

    has_lidar = False
    if (
            not os.path.exists(f'{container}/{checksum}/processed_lidar') or
            len(os.listdir(f'{container}/{checksum}/processed_lidar')) < 2
    ):
        missing_processedlidar.append(fmc_id)
    else:
        has_lidar = True

    has_video = False
    if check_video_exists(container, checksum, meas_name, 'front'):
        has_video = True
    elif fmcTimeToEpoch(fmc['recordingDate']) <= realWorldCutoffEpoch:
        missing_frontvideo.append(fmc_id)

    if check_video_exists(container, checksum, meas_name, 'preview'):
        has_video = True
    elif fmcTimeToEpoch(fmc['recordingDate']) > realWorldCutoffEpoch:
        raw_preview_available = any(referenceFile['type'] == 'PREVIEW_VIDEO_MERGED' for referenceFile in fmc['referenceFiles'])
        if raw_preview_available:
            missing_previewvideo.append(fmc_id)
        else:
            missing_rawpreviewvideo.append(fmc_id)

    if has_lidar and has_video and has_siametadata:
        add_task.append(sequence)

    return add_task, missing_processedlidar, missing_frontvideo, missing_previewvideo, missing_rawpreviewvideo, missing_siametadata


def main():
    valid_ids_out_file = 'VALID_IDS_OUT_FILE'
    missing_data_out_file = 'MISSING_DATA_OUT_FILE'
    new_tasks_out_file = 'NEW_TASKS_OUT_FILE'
    labeled_out_file = 'LABELED_OUT_FILE'

    organization_name = 'nrcs-2-pf'
    fmc_token = request_fmc_token(organization_name)
    fmc_query = 'Car.licensePlate = "LBXQ6155" and Sequence.recordingDate > "2025-01-01"'
    # fmc_query = 'Car.licensePlate = "LBXQ6155" and Sequence.recordingDate > "2025-01-01" and ReferenceFile.type = "PCAP" and ReferenceFile.type = "JSON_METADATA" and MeasurementFile.path ~ "1P_DE_LBXQ6155_ZEUS" and MeasurementFile.contentType ~ "bytesoup"'
    fmc_sequences = get_sequences(fmc_query, organization_name, fmc_token)

    print(f'Found {len(fmc_sequences)} sequences.')

    blacklisted_sequence_ids = set(get_sequences_in_collection(organization_name, 806, fmc_token))
    print(f'Found {len(blacklisted_sequence_ids)} blacklisted sequences in collection id 806.')

    complete_fmc_sequences = []
    missing_fmc_pcap = []
    # TODO: find a way to check if this is valid
    missing_fmc_metadata = []
    missing_fmc_bytesoup = []
    fmc_blacklisted = []

    for seq in fmc_sequences:
        is_not_blacklisted = seq['id'] not in blacklisted_sequence_ids
        has_pcap = fmc_query_has_reference_file_type(seq, 'PCAP')
        has_fmc_metadata = fmc_query_has_reference_file_type(seq, 'JSON_METADATA')
        has_bytesoup = fmc_query_has_bytesoup(seq)

        if is_not_blacklisted and has_pcap and has_fmc_metadata and has_bytesoup:
            complete_fmc_sequences.append(seq)

        if not is_not_blacklisted:
            fmc_blacklisted.append(seq['id'])

        if not has_pcap:
            missing_fmc_pcap.append(seq['id'])

        if not has_fmc_metadata:
            missing_fmc_metadata.append(seq['id'])

        if not has_bytesoup:
            missing_fmc_bytesoup.append(seq['id'])

    sequences = [Sequence(seq['id'], seq) for seq in complete_fmc_sequences]
    set_sequence_measurement_checksums(sequences)
    set_referenceFileTypes(sequences)
    set_sia_link(sequences)
    set_labeled(sequences)

    labeled_tasks = get_labeled_sequences(sequences)

    add_task = []
    missing_processedlidar = []
    missing_frontvideo = []
    missing_previewvideo = []
    missing_rawpreviewvideo = []
    missing_siametadata = []

    with ProcessPoolExecutor() as executor:
        results = list(tqdm(executor.map(check_fmc, sequences), total=len(fmc_sequences)))
        for at, mpl, mfv, mpv, mrpv, msm in results:
            add_task.extend(at)
            missing_processedlidar.extend(mpl)
            missing_frontvideo.extend(mfv)
            missing_previewvideo.extend(mpv)
            missing_rawpreviewvideo.extend(mrpv)
            missing_siametadata.extend(msm)

    with open(valid_ids_out_file, 'w') as f:
        f.write(json.dumps([str(s._id) for s in add_task]))

    with open(missing_data_out_file, 'w') as f:
        f.write(json.dumps({
            'complete_fmc_sequences': [seq['id'] for seq in complete_fmc_sequences],
            'missing_fmc_pcap': missing_fmc_pcap,
            'missing_fmc_bytesoup': missing_fmc_bytesoup,
            'missing_fmc_metadata': missing_fmc_metadata,
            'fmc_blacklisted': fmc_blacklisted,
            'missing_processedlidar': missing_processedlidar,
            'missing_frontvideo': missing_frontvideo,
            'missing_previewvideo': missing_previewvideo,
            'missing_rawpreviewvideo': missing_rawpreviewvideo,
            'missing_siametadata': missing_siametadata,
        }))

    send_new_tasks(add_task, new_tasks_out_file)

    with open(labeled_out_file, 'w') as f:
        f.write(json.dumps(labeled_tasks))

    sequence_id_to_bolf_path = get_sequence_id_to_bolf_path(sequences, organization_name)
    with open('labeltaskforce_bolfs_latest.json', 'w') as f:
        f.write(json.dumps(sequence_id_to_bolf_path))
    #     # TODO: automate to send to fmc


if __name__ == '__main__':
    main()
