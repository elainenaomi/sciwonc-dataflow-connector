"""
DataStoreClient:

This client will provide a interface to connect to the DataStoreServer.
According to a config file, it will be able to create the search query and the write query for retrieval and persistency of data.

"""




class DataStoreClient:

    server = None

    def __init__(self,type, config):
        print "Init DataStore"
        from DataStoreServer import DataStoreServer
        from DataStoreFactory import DataStoreFactory
        self.server = DataStoreServer(DataStoreFactory,type, config);

    def getData(self):
        print "Getting data from server"
        return self.server.getData()

    def saveData(self, data, filename=""):
        print "Saving data to server"
        return self.server.saveData(data, filename)
