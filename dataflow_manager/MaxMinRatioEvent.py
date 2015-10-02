"""
This activity will calculate the mean of ratios between CPU request and Memory request by each event type.
These fields are optional and could be null.
"""

# It will connect to DataStoreClient
from dataflow_components.DataStoreClient import DataStoreClient
from dataflow_components.DataStoreServer import DataStoreServer
from dataflow_components.DataStoreFactory import DataStoreFactory
import ConfigMongoDB_Ratio

# connector and config
client = DataStoreClient(DataStoreServer, DataStoreFactory,"mongodb", ConfigMongoDB_Ratio)

# according to config
data = client.getData() # return an array of docs (like a csv reader)
output = []
max_event = None
max_ratio = 0
min_event = None
min_ratio = 100000000000


if(data):

    # processing
    for doc in data:
        print doc
        ratio = doc['mean ratio cpu memory']
        event = doc['event type']

        if(ratio):
            if(max_ratio < ratio):
                max_ratio = ratio
                max_event = event
            if(min_ratio > ratio):
                min_ratio = ratio
                min_event = event


    newline = {}
    newline['event type max'] = max_event
    newline['ratio max'] = max_ratio
    newline['event type min'] = min_event
    newline['ratio min'] = min_ratio
    output.append(newline)

    # save
    client.saveData(output)
