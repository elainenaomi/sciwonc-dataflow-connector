#!/usr/bin/env python
"""
This activity will calculate the ratio between CPU request and Memory request by (job ID, task index, event type).
These fields are optional and could be null.
"""

# It will connect to DataStoreClient
from sciwonc.dataflow.DataStoreClient import DataStoreClient
import ConfigMongoDB

# connector and config
client = DataStoreClient("mongodb", ConfigMongoDB)

# according to config
data = client.getData() # return an array of docs (like a csv reader)
output = []

if(data):
    # processing
    for doc in data:

        if((doc['CPU request']) and (doc['memory request'])):
            cpu = float(doc['CPU request'])
            memory = float(doc['memory request'])
            ratio = cpu/memory
        else:
            ratio = None

        newline = {}
        newline['job ID'] = doc['job ID']
        newline['task index'] = doc['task index']
        newline['event type'] = doc['event type']
        newline['time'] = doc['time']
        newline['ratio cpu memory'] = ratio

        output.append(newline)


    # save
    client.saveData(output)
