"""
DataStoreFactory:

This class instanciate the concrete classes for NoSQL data stores.

"""
class DataStoreFactory():
    """Abstract Factory"""

    @classmethod
    def getFactory(Class,type, config):
        if type == "mongodb":
            from DataStoreMongoDB import DataStoreMongoDB
            return DataStoreMongoDB()
        elif type == "csv.gz":
            from DataStoreCSVGZ import DataStoreCSVGZ
            return DataStoreCSVGZ()
        else:
            raise NotImplementedError

    class AbstractDataStore:
        """ Abstract Product"""
        def __init__(self, config):
            raise NotImplementedError

        def connection(self):
            raise NotImplementedError

        def getDataByUnit(self, first, last, attributes,sort):
            raise NotImplementedError

        def getDataGroupByColumn(self, column, value, attributes, sort):
            raise NotImplementedError

        def getDataAll(self,attributes, sort):
            raise NotImplementedError

        def getDataGroupByFilename(self,filename):
            raise NotImplementedError

        def saveData(self, data,filename):
            raise NotImplementedError

    @classmethod
    def getDataStore(Class, config):
        return Class.AbstractDataStore(config)
