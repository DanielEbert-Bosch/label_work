import json

import requests
import os

test_data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')

REST_API_URL = 'http://localhost:7100'
REST_API_HEADERS = {
    'accept': 'application/json',
    'Content-Type': 'application/json'
}

with open(os.path.join(test_data_path, 'sia_urls.txt')) as f:
    sia_urls = [line.strip() for line in f.readlines() if line]

tasks = []
for i, sia_url in enumerate(sia_urls):
    tasks.append({
        'fmc_id': str(i),
        'fmc_data': str(i),
        'measurement_checksum': str(i),
        'sia_meas_id_path': sia_url
    })

def log_test(func):
    def wrapper(*args, **kwargs):
        ret = func(*args, **kwargs)
        print(f'Test {func.__name__} successful.')
        return ret
    return wrapper


@log_test
def add_tasks():
    r = requests.post(f'{REST_API_URL}/api/add_tasks', headers=REST_API_HEADERS, data=json.dumps(tasks))
    assert r.status_code == 200, r.text
    assert r.json()['created_tasks'] == tasks

    r = requests.get(f'{REST_API_URL}/api/metrics', headers=REST_API_HEADERS)
    assert r.status_code == 200, r.text
    assert r.json() == {
        'total_labelable': len(tasks),
        'labeled': 0,
        'not_labeled': len(tasks),
        'opened': 0,
        'opened_pending': 0
    }, r.text


@log_test
def get_task():
    r = requests.get(f'{REST_API_URL}/api/get_task?labeler_name=user')
    assert r.status_code == 200, r.text

    r = requests.get(f'{REST_API_URL}/api/metrics', headers=REST_API_HEADERS)
    assert r.status_code == 200, r.text
    assert r.json() == {
        'total_labelable': len(tasks),
        'labeled': 0,
        'not_labeled': len(tasks),
        'opened': 1,
        'opened_pending': 1
    }, r.text


@log_test
def set_labeled():
    labeled_tasks = [
        {
            'measurement_checksum': tasks[3]['measurement_checksum'],
            'label_bolf_path': tasks[3]['sia_meas_id_path']
        }
    ]

    r = requests.post(f'{REST_API_URL}/api/set_labeled', headers=REST_API_HEADERS, data=json.dumps(labeled_tasks))
    assert r.status_code == 200, r.text
    assert r.json() == labeled_tasks, r.text

    r = requests.get(f'{REST_API_URL}/api/metrics', headers=REST_API_HEADERS)
    assert r.status_code == 200, r.text
    assert r.json() == {
        'total_labelable': len(tasks),
        'labeled': 1,
        'not_labeled': len(tasks) - 1,
        'opened': 1,
        'opened_pending': 0
    }, r.text


@log_test
def get_leaderboard():
    r = requests.get(f'{REST_API_URL}/api/leaderboard', headers=REST_API_HEADERS)
    assert r.status_code == 200, r.text
    assert r.json() == {
        'user': 1
    }, r.text


@log_test
def get_index():
    r = requests.get(f'{REST_API_URL}', headers=REST_API_HEADERS)
    assert r.status_code == 200, r.text


@log_test
def skip_task():
    skipped_task = {
        'sia_link': 'https://qa.sia.bosch-automotive-mlops.com/?time=46300&minimalLabelMode=true&minimalLabelModeUser=user&stream=vhm&signal=velocityInMPerS&measId=%2Fdyperexprod%2Fnrcs-2-pf%2Fbeea0130a88cf11be3e4e8980e4cb9f669e450e3189bf698615181765c1516e1%2F1P_DE_LBXQ6155_ZEUS_20250122_153421__HC.bytesoup_lz4.rof',
        'skip_reason': 'wrong_metadata'
    }

    r = requests.post(f'{REST_API_URL}/api/skip_task', headers=REST_API_HEADERS, data=json.dumps(skipped_task))
    assert r.status_code == 200, r.text
    assert r.json()['sia_link'] == skipped_task['sia_link'], r.text


if __name__ == '__main__':
    add_tasks()
    get_task()
    set_labeled()
    get_leaderboard()
    get_index()
    skip_task()

    print('All tests successful.')
