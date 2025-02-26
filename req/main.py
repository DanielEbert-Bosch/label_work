import requests
import json

REST_API_URL = 'http://fe-c-017ev.lr.de.bosch.com:7100'

REST_API_HEADERS = {
    'accept': 'application/json',
    'Content-Type': 'application/json'
}

with open('req_add_tasks.json') as f:
    tasks = f.read()

r = requests.post(f'{REST_API_URL}/api/add_tasks', headers=REST_API_HEADERS, data=tasks)
if r.status_code == 200:
  print('send new tasks successful')


with open('req_set_labeled.json') as f:
    labeled = f.read()

r = requests.post(f'{REST_API_URL}/api/set_labeled', headers=REST_API_HEADERS, data=labeled)
if r.status_code == 200:
    print('send new tasks successful')
