from fmc_api import request_fmc_token, get_sequences

organization_name = 'nrcs-2-pf'
fmc_token = request_fmc_token(organization_name)
# 'Sequence.name = "1P_DE_LBXQ6155_ZEUS_20250205_171724__HC"'
# fmc_query = 'MeasurementFile.checksum = "06120852e9ace6ce4285dc8943c0ea362c7b843cc7bb0efa4251fc778a8fa014"'
# fmc_query = 'Sequence.name ~ "Z_LBXO1994_C1_DEV_ARGUS_LIDAR_MTA2.0_Recording"'
fmc_query = 'Car.licensePlate = "LBXQ6155" and Sequence.recordingDate > "2025-01-01" and ReferenceFile.type = "PCAP" and ReferenceFile.type = "JSON_METADATA"'

fmc_sequences = get_sequences(fmc_query, organization_name, fmc_token)

breakpoint()
