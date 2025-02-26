from azure.identity import AzureCliCredential
from azure.core.credentials import TokenCredential, AccessToken
from requests import get
import argparse
import time
import logging
from glob import glob
import os
from datetime import datetime
import json
import functools
import multiprocessing as mp
import tempfile
import subprocess


logger = logging.getLogger(__name__)

def calc_time(string):
    global start_time

    if string == "start":
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
      calc_time("start")
      self._token[scope] = token = self.delegate.get_token(scope, **kwargs)
      elapsed_time = calc_time("end")
      self.logger.info(
            f"Time taken to generate token(CachedCredential) is {elapsed_time:.2f} seconds."
        )
    else:
        self.logger.info(
            f"Valid token exists"
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


def get_checksums(fmc_query):
    organization_name = 'nrcs-2-pf'
    fmc_token = request_fmc_token(organization_name)
    fmc_sequences = get_sequences(fmc_query, organization_name, fmc_token)

    checksums = []
    for sequence in fmc_sequences:
        checksum = None
        for meas_file in sequence['measurementFiles']:
            if 'bytesoup' in meas_file['path']:
                checksum = meas_file['checksum']

        if checksum is not None:
            checksums.append(checksum)
        else:
            print(f'Failed to find checksum for id', sequence['id'])
    
    return checksums


def get_latest_bolf(bolfs, bot_username):
    latest_bolf = None
    latest_bolf_time = 0

    for bolf_path in bolfs:
        filename = os.path.basename(bolf_path)
        if bot_username in filename:
            # exclude auto generated files
            continue

        creation_date = os.path.basename(os.path.dirname(bolf_path))
        epoch = datetime.strptime(creation_date, "%Y_%m_%d_%H_%M_%S").timestamp()
        if epoch >= latest_bolf_time:
            epoch = latest_bolf_time
            latest_bolf = bolf_path
    
    return latest_bolf


def update_bolf(
        checksum,
        expected_obstacle_type,
        expected_obstacle_height,
        new_obstacle_height,
        current_date_str,
        bot_username,
        blobstore_mount_folder,
        out_dir,
):
    dev_bolfs = glob(f'{blobstore_mount_folder}/dypersiadev/nrcs-2-pf/{checksum}/processed_lidar/*/*.json')
    qua_bolfs = glob(f'{blobstore_mount_folder}/dypersiaqua/nrcs-2-pf/{checksum}/processed_lidar/*/*.json')
    bolfs = dev_bolfs + qua_bolfs

    latest_bolf = get_latest_bolf(bolfs, bot_username)
    if not latest_bolf:
        # print(f'Did not find latest bolf for {checksum=}. Likely not labeled yet.')
        return

    with open(latest_bolf) as f:
        bolf = json.loads(f.read())
    
    obstacle_type = list(bolf['openlabel']['objects'].values())[0]['type']

    if obstacle_type != expected_obstacle_type:
        return

    did_update = False
    for frame in bolf['openlabel']['frames'].values():
        for object in frame['objects'].values():
            height_attribute = object['object_data']['poly3d'][0]['attributes']['num'][0]
            if height_attribute['name'] != 'height':
                print('Warning: found non height attribute.')
                continue
            current_height_val = height_attribute['val']
            if expected_obstacle_height != str(current_height_val).strip():
                continue
            if type(current_height_val) == str:
                height_attribute['val'] = new_obstacle_height
                did_update = True
            elif type(current_height_val) in [float, int]:
                height_attribute['val'] = float(new_obstacle_height)
                did_update = True

    if not did_update:
        return
    
    new_bolf_name = f'{checksum}_{bot_username}_{bot_username}_{current_date_str}.json'
    bolf_folderpath = os.path.dirname(latest_bolf)[len(blobstore_mount_folder) + 1:]
    new_bolf_folderpath = os.path.join(out_dir, bolf_folderpath)
    new_bolf_path = os.path.join(new_bolf_folderpath, new_bolf_name)

    os.makedirs(new_bolf_folderpath, exist_ok=True)

    with open(new_bolf_path, 'w') as f:
        f.write(json.dumps(bolf))

    print('Created', new_bolf_path)


def main():
    # TODO: add option for new obstacle type
    parser = argparse.ArgumentParser()
    parser.add_argument('--fmc_query', required=True)
    parser.add_argument('--expected_obstacle_type', required=True)
    parser.add_argument('--expected_obstacle_height', required=True)
    parser.add_argument('--new_obstacle_height', required=True)
    parser.add_argument('--blobstore_mount_folder', default='/home/jovyan/data/ReadOnly/', help='Path to folder where dypersiadev and dypersiaqua blob store folders are mounted.')
    parser.add_argument('--output_targz_file', default='updated_bolfs.tar.gz')
    args = parser.parse_args()

    bot_username = 'autogen'
    current_date_str = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

    checksums = get_checksums(args.fmc_query)

    with tempfile.TemporaryDirectory() as tmp_dir:
        partial_update_bolf = functools.partial(
            update_bolf,
            expected_obstacle_type=args.expected_obstacle_type,
            expected_obstacle_height=args.expected_obstacle_height,
            new_obstacle_height=args.new_obstacle_height,
            current_date_str=current_date_str,
            bot_username=bot_username,
            blobstore_mount_folder=args.blobstore_mount_folder,
            out_dir=tmp_dir
        )

        with mp.Pool() as pool:
            pool.map(partial_update_bolf, checksums)

        subprocess.check_call(['tar', 'czf', args.output_targz_file, '-C', tmp_dir, '.'])

if __name__ == '__main__':
    raise SystemExit(main())
