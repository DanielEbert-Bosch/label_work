# Bolf Update Script

Example
~~~
python generate_bolfs.py --fmc_query 'Car.licensePlate = "LBXQ6155" and Sequence.recordingDate > "2025-02-09" and ReferenceFile.type = "PCAP" and ReferenceFile.type = "JSON_METADATA"' --expected_obstacle_type curbstone_real_normal --expected_obstacle_height 0.16 --new_obstacle_height 99
~~~

###

maybe send from server over websocket to notebook
