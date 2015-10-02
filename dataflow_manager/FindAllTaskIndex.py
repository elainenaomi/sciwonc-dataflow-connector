# It will connect to DataStoreClient
from dataflow_components.DataStoreClient import DataStoreClient
from dataflow_components.DataStoreServer import DataStoreServer
from dataflow_components.DataStoreFactory import DataStoreFactory

import Config_FileTaskIndex


# connector and config
clientTask = DataStoreClient(DataStoreServer, DataStoreFactory,"mongodb", Config_FileTaskIndex)
# according to config
tasks = clientTask.getData() # return an array of docs



if(tasks):

    output = []
    jobNameIdTaskTupleList = []
    i = 0

    for doc in tasks:

        newTuple = (str(doc['job ID']), str(doc['task index']))

        if(newTuple not in jobNameIdTaskTupleList):
            jobNameIdTaskTupleList.append(newTuple)


    for i, (b,c) in enumerate(jobNameIdTaskTupleList):
        print i
        print (b,c)
        newDoc = {"job ID":b, "task index":c}
        output.append(newDoc)

    # save
    clientTask.saveData(output)
