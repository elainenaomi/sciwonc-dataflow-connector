"""
This activity will calculate the mean of ratios between CPU request and Memory request by each event type.
These fields are optional and could be null.
"""

# It will connect to DataStoreClient
from dataflow_components.DataStoreClient import DataStoreClient
from dataflow_components.DataStoreServer import DataStoreServer
from dataflow_components.DataStoreFactory import DataStoreFactory
import ConfigMongoDB_TimeEvent

# connector and config
client = DataStoreClient(DataStoreServer, DataStoreFactory,"mongodb", ConfigMongoDB_TimeEvent)
# according to config
data = client.getData() # return an array of docs (like a csv reader)



output = []
taskList = []


if(data):

    # processing
    for doc in data:


        #print doc


'''
        total_tasks += 1

        if(doc['ratio cpu memory']):
            sum_ratio = sum_ratio + float(doc['ratio cpu memory'])
            total_valid_tasks += 1


    newline = {}
    newline['event type'] = doc['event type']
    newline['sum ratio cpu memory'] = sum_ratio
    newline['total valid tasks'] = total_valid_tasks
    newline['total tasks'] = total_tasks
    if((sum_ratio > 0) and (total_valid_tasks > 0)):
        newline['mean ratio cpu memory'] = sum_ratio / total_valid_tasks
    else:
        newline['mean ratio cpu memory'] = None

    output.append(newline)


    # save
    client.saveData(output)
'''
