from datetime import datetime
import os
import requests

from jupyterclient import JupyterClient


REST_API_URL = 'http://fe-c-017ev.lr.de.bosch.com:7100'

REST_API_HEADERS = {
    'accept': 'application/json',
    'Content-Type': 'application/json'
}


def get_datetime_filename_string():
    """Returns current datetime string YYYY-MM-DD_HH-MM-SS-ms."""
    now = datetime.now()
    return now.strftime('%Y-%m-%d_%H-%M-%S-') + f'{now.microsecond // 1000:03d}'


def main():
    src_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'blobstore_query.py')
    with open(src_path) as f:
        blobstore_query_src = f.read()

    params = [
        'VALID_IDS_OUT_FILE',
        'MISSING_DATA_OUT_FILE',
        'NEW_TASKS_OUT_FILE',
        'LABELED_OUT_FILE',
    ]

    datestr = get_datetime_filename_string()
    param_outpaths = {}
    for param in params:
        param_outpaths[param] = datestr + '_' + param.lower() + '.json'

    for param, filepath in param_outpaths.items():
        blobstore_query_src = blobstore_query_src.replace(param.upper(), filepath)

    jc = JupyterClient(print)
    jc.exec(blobstore_query_src)

    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../out')

    params_output = {}
    for param, filepath in param_outpaths.items():
        params_output[param] = jc.download(filepath)
        with open(os.path.join(out_dir, f'{param.lower()}.json'), 'w') as f:
            f.write(params_output[param]['content'])

    print('completed')
    jc.close()

    r = requests.post(f'{REST_API_URL}/api/add_tasks', headers=REST_API_HEADERS, data=params_output['NEW_TASKS_OUT_FILE']['content'])
    if r.status_code == 200:
        print('send new tasks successful')

    r = requests.post(f'{REST_API_URL}/api/set_labeled', headers=REST_API_HEADERS, data=params_output['LABELED_OUT_FILE']['content'])
    if r.status_code == 200:
        print('send new tasks successful')
    
    print('finished')


if __name__ == '__main__':
    raise SystemExit(main())