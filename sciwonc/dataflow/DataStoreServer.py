#!/usr/bin/env python
"""
DataStoreServer:

This class will be responsible for the mediaton between the DataStoreClient and the real data store used to manage the data.
It will instanciate an Abstract Factory and calls the concrete factory.

"""

from DataStoreFactory import DataStoreFactory

class DataStoreServer(DataStoreFactory):
    factory = None
    connection = None
    config =  None
    numline = 0

    def __init__(self,DataStoreFactory,type, config):
        """Application"""
        self.config = config
        self.factory = DataStoreFactory.getFactory(type,config)

    def getData(self):
        if(self.config.OPERATION_TYPE == "UNIT"):
            return self.factory.getDataStore(self.config).getDataByUnit(self.config.FIRST_ITEM, self.config.LAST_ITEM, self.config.ATTRIBUTES, self.config.SORT)
        elif(self.config.OPERATION_TYPE == "GROUP_BY_COLUMN"):
            return self.factory.getDataStore(self.config).getDataGroupByColumn(self.config.COLUMN, self.config.VALUE, self.config.ATTRIBUTES, self.config.SORT)
        elif(self.config.OPERATION_TYPE == "ALL"):
            return self.factory.getDataStore(self.config).getDataAll(self.config.ATTRIBUTES, self.config.SORT)
        elif(self.config.OPERATION_TYPE == "GROUP_BY_FILENAME"):
            return self.factory.getDataStore(self.config).getDataGroupByFilename(self.config.FILENAME)
        elif(self.config.OPERATION_TYPE == "DISTINCT"):
            return self.factory.getDataStore(self.config).getDataDistinct(self.config.COLUMN)
        elif(self.config.OPERATION_TYPE == "GROUP_BY_FIXED_WINDOW"):
            return self.factory.getDataStore(self.config).getDataGroupByFixedWindow(self.config.COLUMN, self.config.VALUE, self.config.ATTRIBUTES, self.config.SORT)
        else:
            return None

    def saveData(self, data, filename, start_numline = 1):
        self.numline = start_numline

        if type(data) is dict:
            data = [data]

        return self.factory.getDataStore(self.config).saveData(data, filename, self.numline)
