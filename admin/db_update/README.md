# DB Update

Adds new label tasks to the task list at http://fe-c-017ev.lr.de.bosch.com:7100/ and checks for created labels in the blob stores.

### Install

Generate notebook token from here: https://notebook.bosch-automotive-mlops.com/hub/token

Create .env file with this content and replace the values with your jupyter username and token:
~~~
JUPYTER_USERNAME=ebd7rng@bosch.com
JUPYTER_TOKEN=00000000000000000000000000000000
~~~

~~~
python3 -m venv venv
. venv/bin/activate
pip install -r requirements.txt
~~~

### Run

- Go to https://notebook.bosch-automotive-mlops.com 
- Start the `KPI-Postprocessing` notebook
- In the notebook, run: az login


Locally, run:
~~~
. venv/bin/activate
python src/main.py
~~~

# DB Blacklist Update

Set fmc collection id 806 as the label task blacklist.

### Install

See Install step from 'DB Update'

### Run

~~~
. venv/bin/activate
python src/set_fmc_blacklist.py
~~~
