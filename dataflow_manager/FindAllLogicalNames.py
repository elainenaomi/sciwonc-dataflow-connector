"""
This activity will calculate the ratio between CPU request and Memory request by (job ID, task index, event type).
These fields are optional and could be null.
"""

# It will connect to DataStoreClient
from dataflow_components.DataStoreClient import DataStoreClient
from dataflow_components.DataStoreServer import DataStoreServer
from dataflow_components.DataStoreFactory import DataStoreFactory
import Config_AllLogicalNames

# connector and config
client = DataStoreClient(DataStoreServer, DataStoreFactory,"mongodb", Config_AllLogicalNames)

# according to config
data = client.getData() # return an array of docs (like a csv reader)
output = []
jobnames = []
i = 0

if(data):
    # processing
    for doc in data:
        #print doc['logical job name']

        if(doc['logical job name'] not in jobnames):
            jobnames.append(doc['logical job name'])

            newline = {}
            newline['logical job name'] = doc['logical job name']
            output.append(newline)

            i += 1

    print i

    # save
    client.saveData(output)
