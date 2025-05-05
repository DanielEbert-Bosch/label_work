from __future__ import annotations

try:
    from azure.identity import AzureCliCredential
    from azure.core.credentials import TokenCredential, AccessToken
except ModuleNotFoundError:
    import os
    os.system('pip install azure-identity')
    from azure.identity import AzureCliCredential
    from azure.core.credentials import TokenCredential, AccessToken


from requests import get
import time
import os
import requests
import json

import logging
logger = logging.getLogger(__name__)


DO_TESTING = os.getenv('DO_TESTING', '0') != '0'

if DO_TESTING:
    REST_API_URL = 'http://localhost:7100'
else:
    REST_API_URL = 'http://fe-c-017ev.lr.de.bosch.com:7100'

REST_API_HEADERS = {
    'accept': 'application/json',
    'Content-Type': 'application/json'
}


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


def chunk_array(arr, x):
    return [arr[i:i + x] for i in range(0, len(arr), x)]


def set_fmc_blacklist():
    organization_name = 'nrcs-2-pf'
    fmc_token = request_fmc_token(organization_name)
    blacklisted_sequence_ids = get_sequences_in_collection(organization_name, 806, fmc_token)

    fmc_ids = list(str(i) for i in blacklisted_sequence_ids)
    fmc_sequences = []
    for fmc_ids_chunk in chunk_array(fmc_ids, 100):
        fmc_ids_str = '","'.join(fmc_ids_chunk)
        fmc_sequences.extend(get_sequences(f'Sequence.id in ["{fmc_ids_str}"]', organization_name, fmc_token))
    
    checksums = []
    for sequence in fmc_sequences:
        checksum = None
        for meas_file in sequence['measurementFiles']:
            if 'bytesoup' in meas_file['path']:
                checksum = meas_file['checksum']

        if checksum is not None:
            checksums.append(checksum)
 
    r = requests.post(f'{REST_API_URL}/api/set_fmc_blacklist', headers=REST_API_HEADERS, data=json.dumps(checksums))
    if r.status_code == 200:
        print('send new tasks successful')
    else:
        breakpoint()
        print(f'request failed with {r.status_code=}')


if __name__ == '__main__':
    raise SystemExit(set_fmc_blacklist())
