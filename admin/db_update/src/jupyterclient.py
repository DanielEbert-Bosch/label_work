import json
import uuid
import websocket
import requests
import logging as log
import os
import time
from typing import Callable
from dotenv import load_dotenv

load_dotenv()

JUPYTER_SERVER_URL = os.getenv('JUPYTER_SERVER_URL', 'https://notebook.bosch-automotive-mlops.com')
JUPYTER_USERNAME = os.getenv('JUPYTER_USERNAME', 'ebd7rng@bosch.com')
JUPYTER_TOKEN = os.getenv('JUPYTER_TOKEN')
HEADERS = {'Authorization': f'Bearer {JUPYTER_TOKEN}'}

level_str = os.getenv('LOG_LEVEL', 'INFO').upper()
level = getattr(log, level_str, log.INFO)
log.basicConfig(level=level, format='%(levelname)s: %(message)s')
logger = log.getLogger(__name__)


class JupyterClient:
    def __init__(self, add_log: Callable[[str], None] | None = None):
        if add_log: 
            self.add_log = add_log
        else:
            self.add_log = log.info

        server_status = self.server_status()
        if server_status == 'offline':
            raise Exception(f'Cannot connect to jupyter server. Server is offline. Start server on page {JUPYTER_SERVER_URL}')
        while server_status == 'starting':
            self.add_log('Waiting for server to start...')
            time.sleep(2)
            server_status = self.server_status()
        self.add_log('Server is running.')

        response = requests.post(f'{JUPYTER_SERVER_URL}/user/{JUPYTER_USERNAME}/api/kernels', headers=HEADERS)
        if response.status_code != 201 or response.json()['name'] != 'python3':
            raise Exception(f"Kernel creation failed: {response.text}")
        self.kernel_id = response.json()['id']

        ws_server_url = JUPYTER_SERVER_URL.replace('https', 'wss')
        self.ws = websocket.create_connection(f'{ws_server_url}/user/{JUPYTER_USERNAME}/api/kernels/{self.kernel_id}/channels?token={JUPYTER_TOKEN}')
        self.add_log(f'Connected to websocket {ws_server_url}')

    def exec(self, code: str) -> None:
        msg_id = str(uuid.uuid4())
        execute_request = {
            "header": {
                "msg_id": msg_id,
                "username": JUPYTER_USERNAME,
                "session": msg_id,
                "msg_type": "execute_request",
                "version": "5.2"
            },
            "parent_header": {},
            "metadata": {},
            "content": {
                "code": code,
                "silent": False,
                "store_history": True,
                "user_expressions": {},
                "allow_stdin": False
            },
            "channel": "shell"
        }
        self.ws.send(json.dumps(execute_request))

        while True:
            msg = json.loads(self.ws.recv())
            if msg.get("parent_header", {}).get("msg_id") == msg_id:
                self.add_log(msg)
                if msg.get('msg_type') == 'execute_reply':
                    break
        
        print('exec done')
    
    def exec_file(self, path: str):
        with open(path) as f:
            self.exec(f.read())
        self.add_log(f'Executed file {path}')

    def server_status(self):
        response = requests.get(f'{JUPYTER_SERVER_URL}/hub/api/users/{JUPYTER_USERNAME}', headers=HEADERS)
        if response.status_code != 200:
            raise Exception(f'Failed to get server status: {response.status_code}')
        body = response.json()
        if body['server'] == None:
            return 'offline'
        
        if '' in body['servers'] and not body['servers']['']['ready']:
            return 'starting'

        if '' in body['servers'] and body['servers']['']['ready']:
            return 'running'
 
        raise Exception('Unknown server state.', body)
    
    def start_server(self):
        response = requests.post(f'{JUPYTER_SERVER_URL}/hub/api/users/{JUPYTER_USERNAME}/servers/', headers=HEADERS)
        if response.status_code != 202:
            raise Exception(f'Failed to get server status: {response.status_code}')
    
    def download(self, path: str):
        response = requests.get(f'{JUPYTER_SERVER_URL}/user/{JUPYTER_USERNAME}/api/contents/{path}', headers=HEADERS)
        if response.status_code != 200:
            raise Exception(f'Download failed: {response.status_code}')
        return response.json()
    
    def upload(self, path: str, content: str):
        data = {'content': content, 'format': 'text', 'type': 'file'}
        response = requests.put(
            f'{JUPYTER_SERVER_URL}/user/{JUPYTER_USERNAME}/api/contents/{path}',
            headers=HEADERS,
            data=json.dumps(data)
        )
        if response.status_code not in [200, 201]:
            raise Exception(f'Upload failed: {response.status_code}')
        self.add_log(f'Uploaded file {path}')
        return response.json()

    def create_folder(self, path: str):
        data = {'type': 'directory'}
        response = requests.put(
            f'{JUPYTER_SERVER_URL}/user/{JUPYTER_USERNAME}/api/contents/{path}',
            headers=HEADERS,
            data=json.dumps(data)
        )
        if response.status_code not in [200, 201]:
            raise Exception(f'Upload failed: {response.status_code}')
        self.add_log(f'Created folder {path}')
        return response.json()
    
    def delete_file(self, path: str):
        response = requests.delete(f'{JUPYTER_SERVER_URL}/user/{JUPYTER_USERNAME}/api/contents/{path}', headers=HEADERS)
        if response.status_code != 204:
            raise Exception(f'Delete failed: {response.status_code}')
        self.add_log(f'Deleted file {path}')
 
    def close(self):
        if self.ws:
            self.ws.close()
            self.ws = None
            r = requests.delete(f'{JUPYTER_SERVER_URL}/user/{JUPYTER_USERNAME}/api/kernels/{self.kernel_id}', headers=HEADERS)
            if r.status_code != 204:
                print('Failed to delete kernel id', self.kernel_id, r.text)
