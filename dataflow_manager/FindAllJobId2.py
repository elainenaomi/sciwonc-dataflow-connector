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
clientJobId = DataStoreClient(DataStoreServer, DataStoreFactory,"mongodb", Config_FileJobId)
# according to config
jobs = clientJobId.getData() # return an array of docs

# define a tuple (logical, job id)
# check if not exists
# save like a document after


if(jobs):

    output = []
    jobNameIdTupleList = []
    i = 0

    for doc in jobs:

        newTuple = (str(doc['logical job name']), str(doc['job ID']))

        if(newTuple not in jobNameIdTupleList):
            jobNameIdTupleList.append(newTuple)


    for i, (a, b) in enumerate(jobNameIdTupleList):
        print i
        print (a,b)
        newDoc = {"logical job name":a, "job ID":b}
        output.append(newDoc)

    # save
    clientJobId.saveData(output)
