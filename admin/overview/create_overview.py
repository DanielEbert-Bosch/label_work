import os
import json
from datetime import datetime
import requests

from fmc_api import request_fmc_token, get_sequences

REST_API_URL = 'http://fe-c-017ev.lr.de.bosch.com:7100'
REST_API_HEADERS = {
    'accept': 'application/json',
    'Content-Type': 'application/json'
}

def chunk_list(input_list, chunk_size=50):
    """Split a list into chunks of specified size."""
    return [input_list[i:i + chunk_size] for i in range(0, len(input_list), chunk_size)]

def get_fmc_bolf_path(sequence) -> str | None:
    """Extract the BOLF path from a sequence."""
    for label_file in sequence['labelFiles']:
        if label_file['domain'] == 'KPI_GENERATED_BOLF':
            return label_file['path']
    return None

def load_data_from_files(out_dir):
    """Load and process data from JSON files."""
    data = {}
    
    with open(os.path.join(out_dir, 'missing_data_out_file.json')) as f:
        missing_data = json.loads(f.read())
    
    data['bs + pcap + meta'] = missing_data['complete_fmc_sequences']
    data['Missing Pcap'] = missing_data['missing_fmc_pcap']
    data['Missing Bytesoup'] = missing_data['missing_fmc_bytesoup']
    data['Missing FMC Metadata'] = missing_data['missing_fmc_metadata']
    data['Missing FMC Previewvideo'] = missing_data['missing_rawpreviewvideo']
    data['FMC Sequence Blacklisted'] = missing_data['fmc_blacklisted']
    
    with open(os.path.join(out_dir, 'new_tasks_out_file.json')) as f:
        data['pcd + sia mp4 & meta'] = [int(seq['fmc_id']) for seq in json.loads(f.read())]
 
    data['Missing Sia Previewvideo'] = missing_data['missing_previewvideo']
    data['Missing Lidarvideo'] = missing_data['missing_frontvideo']
    data['Missing Sia Metadata'] = missing_data['missing_siametadata']
    data['Missing Lidar PCD'] = missing_data['missing_processedlidar']
    
    with open(os.path.join(out_dir, 'labeled_out_file.json')) as f:
        labeled_checksums = json.loads(f.read())
    
    checksum_to_bolf_url = {}
    for label in labeled_checksums:
        checksum_to_bolf_url[label['measurement_checksum']] = label['label_bolf_path']
    
    return data, missing_data, checksum_to_bolf_url

def fetch_metrics_data(data):
    """Fetch metrics data from the REST API."""
    r = requests.get(f'{REST_API_URL}/api/metrics_with_sequences', headers=REST_API_HEADERS)
    assert r.status_code == 200, r.text
    metrics = r.json()

    data['Labeled'] = metrics['labeled']
    data['Not Labeled'] = metrics['not_labeled']
    data['Opened Pending'] = metrics['opened_pending']
    
    r = requests.get(f'{REST_API_URL}/api/blacklist', headers=REST_API_HEADERS)
    assert r.status_code == 200, r.text
    blacklist = r.json()
    data['Label Blacklisted'] = blacklist['sequences']
 
    return data

def analyze_fmc_linking(checksum_to_bolf_url, organization_name):
    """Analyze which FMC sequences are linked correctly."""
    fmc_token = request_fmc_token(organization_name)
    fmc_query = 'Car.licensePlate = "LBXQ6155" and Sequence.recordingDate > "2025-01-01" and ReferenceFile.type = "PCAP" and ReferenceFile.type = "JSON_METADATA" and MeasurementFile.contentType ~ "bytesoup"'
    fmc_sequences = get_sequences(fmc_query, organization_name, fmc_token)

    labeled_checksums = set(checksum_to_bolf_url.keys())

    label_fmc_linked = []
    label_fmc_unlinked = []

    for seq in fmc_sequences:
        is_linked = False
        skip = True
        for meas_file in seq['measurementFiles']:
            if 'bytesoup' not in meas_file['path']:
                continue
            if meas_file['checksum'] not in labeled_checksums:
                continue
        
            skip = False
            if checksum_to_bolf_url[meas_file['checksum']] == get_fmc_bolf_path(seq):
                is_linked = True
        
        if skip:
            continue

        if is_linked:
            label_fmc_linked.append(seq['id'])
        else:
            label_fmc_unlinked.append(seq['id'])
 
    return label_fmc_linked, label_fmc_unlinked


def things_that_shouldnt_be_done_here(data):
    ds = {k: set(v) for k, v in data.items()}
    stage1_blacklist = ds['Missing Pcap'] | ds['Missing FMC Metadata'] | ds['FMC Sequence Blacklisted'] | ds['Missing Bytesoup']

    stage3keys = ['Labeled', 'Not Labeled', 'Opened Pending', 'Label Blacklisted']
    for k in stage3keys:
        data[k] = list(set(data[k]) - stage1_blacklist)

    ds = {k: set(v) for k, v in data.items()}
    
    stage3_blacklist = ds['Not Labeled'] | ds['Opened Pending'] | ds['Label Blacklisted']
    stage4keys = ['Bolf FMC Linked', 'Bolf FMC Unlinked']
    for k in stage4keys:
        data[k] = list(set(data[k]) - stage3_blacklist)



def update_html_file(seq_id_data, output_filename='out.html'):
    """Update the specification file with the current data."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    template_path = os.path.join(script_dir, 'plot.html')
    
    with open(template_path) as f:
        html = f.read()    

    with open('data_seq_ids.json', 'w') as f:
        f.write(json.dumps(seq_id_data))
 
    data = {key: str(len(value)) for key, value in seq_id_data.items()}

    html = html.replace('FMC_TO_BS_PCAP_META', data['bs + pcap + meta'])
    html = html.replace('FMC_TO_MISSING_PCAP', data['Missing Pcap'])
    html = html.replace('FMC_TO_MISSING_BYTESOUP', data['Missing Bytesoup'])
    html = html.replace('FMC_TO_MISSING_META', data['Missing FMC Metadata'])
    html = html.replace('FMC_TO_SEQUENCE_BLACKLIST', data['FMC Sequence Blacklisted'])

    html = html.replace('BS_PCAP_META_TO_PCD_MP4_META', data['pcd + sia mp4 & meta'])
    html = html.replace('BS_PCAP_META_TO_MISSING_LIDAR_PCD', data['Missing Lidar PCD'])
    html = html.replace('BS_PCAP_META_TO_MISSING_SIA_META', data['Missing Sia Metadata'])
    html = html.replace('BS_PCAP_META_TO_MISSING_SIA_PREVIEWVIDEO', data['Missing Sia Previewvideo'])
    html = html.replace('BS_PCAP_META_TO_MISSING_LIDARVIDEO', data['Missing Lidarvideo'])
    html = html.replace('BS_PCAP_META_TO_MISSING_FMC_PREVIEWVIDEO', data['Missing FMC Previewvideo'])

    html = html.replace('PCD_MP_META_TO_LABELED', data['Labeled'])
    html = html.replace('PCD_MP_META_TO_NOT_LABELED', data['Not Labeled'])
    html = html.replace('PCD_MP_META_TO_OPENED_PENDING', data['Opened Pending'])
    html = html.replace('PCD_MP_META_TO_LABEL_BLACKLIST', data['Label Blacklisted'])

    html = html.replace('LABELED_TO_LINKED', data['Bolf FMC Linked'])
    html = html.replace('LABELED_TO_UNLINKED', data['Bolf FMC Unlinked'])

    with open(output_filename, 'w') as f:
        f.write(html)

def main():
    """Main function to orchestrate the data processing workflow."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    out_dir = os.path.join(script_dir, '../db_update/out')
    
    data, _, checksum_to_bolf_url = load_data_from_files(out_dir)
    data = fetch_metrics_data(data)
 
    organization_name = 'nrcs-2-pf'
    label_fmc_linked, label_fmc_unlinked = analyze_fmc_linking(checksum_to_bolf_url, organization_name)
    
    data['Bolf FMC Linked'] = label_fmc_linked
    data['Bolf FMC Unlinked'] = label_fmc_unlinked

    data= {k: [int(i) for i in v] for k, v in data.items()}

    things_that_shouldnt_be_done_here(data)
    
    update_html_file(data)

if __name__ == "__main__":
    main()
