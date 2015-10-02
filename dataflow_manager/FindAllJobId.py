"""
This activity will calculate the ratio between CPU request and Memory request by (job ID, task index, event type).
These fields are optional and could be null.
"""

# It will connect to DataStoreClient
from dataflow_components.DataStoreClient import DataStoreClient
from dataflow_components.DataStoreServer import DataStoreServer
from dataflow_components.DataStoreFactory import DataStoreFactory
import Config_FileLogical
import Config_FileJobId

# connector and config
clientLogicalName = DataStoreClient(DataStoreServer, DataStoreFactory,"mongodb", Config_FileLogical)
# according to config
logicalNames = clientLogicalName.getData() # return an array of docs

# connector and config
clientJobId = DataStoreClient(DataStoreServer, DataStoreFactory,"mongodb", Config_FileJobId)
# according to config
jobs = clientJobId.getData() # return an array of docs


if((logicalNames) and (jobs)):


    output = []
    jobId = []
    i = 0


    for doc in jobs:
        #print doc['logical job name']

        for job in logicalNames:

            if(doc['logical job name'] == job['logical job name']):
                #print "eq"
                job['job ID'] = job.get('job ID', [])

                if(doc['job ID'] not in job['job ID']):
                    job['job ID'].append(str(doc['job ID']))
                break;


    # save
    clientJobId.saveData(logicalNames)
