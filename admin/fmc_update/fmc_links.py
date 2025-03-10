import os
import subprocess
from azure.identity import AzureCliCredential
from azure.core.credentials import TokenCredential, AccessToken
from requests import get
import time
import logging
from glob import glob
import json
from concurrent.futures import ProcessPoolExecutor
logger = logging.getLogger(__name__)


recompute_dataloop_utility_repo = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'recompute-dataloop-utility')
config = os.path.join(recompute_dataloop_utility_repo, 'upload_n_link/user_configuration.yaml')


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


def link_fmc_mp4_jpeg(link_json_path: str):
    updated_config = []
    with open(config) as f:
        for line in f.readlines():
            if line.strip().startswith('use_case_config_file_path'):
                updated_config.append(f'use_case_config_file_path : {link_json_path}\n')
            else:
                updated_config.append(line)

    with open(config, 'w') as f:
        f.writelines(updated_config)

    cmd = f'. {recompute_dataloop_utility_repo}/.venv/bin/activate && cd {recompute_dataloop_utility_repo}/upload_n_link && python file_upload_link_main.py'
    subprocess.check_call(cmd, shell=True)


def main():
    organization_name = 'nrcs-2-pf'
    fmc_token = request_fmc_token(organization_name)
    fmc_query = 'Car.licensePlate = "LBXQ6155" and Sequence.recordingDate > "2025-02-09" and ReferenceFile.type = "PCAP" and ReferenceFile.type = "JSON_METADATA"'
    fmc_sequences = get_sequences(fmc_query, organization_name, fmc_token)

    dyperexprod_dir = '/home/jovyan/data/ReadOnly/dyperexprod/nrcs-2-pf'

    checksum_to_fmc_id = {}
    for sequence in fmc_sequences:
        checksum = None
        for meas_file in sequence['measurementFiles']:
            if 'bytesoup' in meas_file['path']:
                checksum = meas_file['checksum']

        if checksum is not None:
            checksum_to_fmc_id[checksum] = sequence['id']


    def process_item(item):
        checksum, fmc_id = item
        mp4_files = glob(f'{dyperexprod_dir}/{checksum}/**/*preview.mp4', recursive=True)
        jpg_files = glob(f'{dyperexprod_dir}/{checksum}/**/*preview.jpg', recursive=True)
        return fmc_id, (mp4_files, jpg_files)

    with ProcessPoolExecutor() as executor:
        id_to_preview = dict(executor.map(process_item, checksum_to_fmc_id.items()))

    out_nrcs = {}
    local_prefix_len = len('/home/jovyan/data/ReadOnly/dyperexprod')

    for fmc_id, (mp4_files, jpg_files) in id_to_preview.items():
        if not mp4_files or not jpg_files:
            continue

        out_nrcs[fmc_id] = {
            'video_path': 'https://dyperexprod.blob.core.windows.net/' + mp4_files[-1][local_prefix_len:],
            'image_path': 'https://dyperexprod.blob.core.windows.net/' + jpg_files[-1][local_prefix_len:]
        }

    out = {
        'nrcs-2-pf': out_nrcs
    }

    with open('preview_mp4_and_jpg.json', 'w') as f:
        f.write(json.dumps(out))

    link_fmc_mp4_jpeg('preview_mp4_and_jpg.json')


if __name__ == '__main__':
    raise SystemExit(main())

# TODO: setup upload script on jnb postproce, checkout commit, and add cli
