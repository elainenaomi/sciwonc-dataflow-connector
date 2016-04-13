#!/usr/bin/env python
"""
This is the concrete factory to manage mongodb servers

"""
from DataStoreFactory import DataStoreFactory
from pymongo import MongoClient
from DataObject import DataObject
from collections import OrderedDict

import pymongo

class DataStoreMongoDB(DataStoreFactory):
    """Concrete Factory"""

    class AbstractDataStore(DataStoreFactory.AbstractDataStore):
        """Concrete Product"""

        config = None
        connection = None
        db = None
        collection_input = None
        collection_output = None
        type = "mongodb"

        def __init__(self,config):
            try:
                print "Init MongoDB"
                self.config = config

            except:
                print "Error: Config file not found"



        def connect(self, documentClass = OrderedDict):
            print "I am a MongoDB Connection"
            print str(documentClass)

            strConnection = 'mongodb://'+self.config.HOST+'/'
            if self.connection is None:

                if (hasattr(self.config, 'READ_PREFERENCE')):
                    readPreference = self.config.READ_PREFERENCE
                else:
                    readPreference = "primary"


                # default document_class=dict
                self.connection = MongoClient(strConnection,readPreference=readPreference,document_class = documentClass )
                print strConnection
                print readPreference
                print self.connection.read_preference


                self.db = getattr(self.connection, self.config.DATABASE)

                if (hasattr(self.config, 'COLLECTION_INPUT')):
                    self.collection_input = getattr(self.db, self.config.COLLECTION_INPUT)

                if (hasattr(self.config, 'COLLECTION_OUTPUT')):
                    self.collection_output = getattr(self.db, self.config.COLLECTION_OUTPUT)


        def getDataByUnit(self, first, last, attributes,sort):

            first["numline"] = int(first["numline"])
            last["numline"] = int(last["numline"])

            #query = {'_id':{'$gte':first, '$lte':last}}
            first = OrderedDict(sorted(first.items()))
            print first

            last = OrderedDict(sorted(last.items()))
            print last

            subQuery = OrderedDict([('$lte',last), ('$gte',first)])

            query = OrderedDict([("_id", subQuery)])
            print query

            projection = {}
            sort_query = []

            print "Getting data from MongoDB"
            print "Unit Method"

            for attribute in attributes:
                projection[attribute] = 'true'
            #.sort(sort)[('numline',pymongo.ASCENDING),('filepath',pymongo.DESCENDING)]

            for attribute in sort:
                sort_query.append((attribute,pymongo.ASCENDING))

            print projection
            print "Query - "+str(query)
            print sort_query
            print "\n"



            try:
                self.connect()
                cursor = self.collection_input.find(query,projection).sort(sort_query)
                return DataObject(self.type, cursor, self.config)
            except Exception as e:
                print "Unexpected error:", type(e), e

        def getQueryString(self, column, value):

            query = {}

            if type(column) is str:

                if type(value) is str:
                    query = {column:value}
                elif type(value) is list:

                    itemList = []
                    for item in value:
                        itemList.append({column:item})

                    query = {'$or':itemList}

            elif type(column) is tuple:

                if type(value) is tuple:

                    if(len(column) == len(value)):
                        for item in range(0, len(value)):
                            query[column[item]] = value[item]

                elif type(value) is list:
                    itemList = []
                    for item in value:
                        if type(item) is tuple:
                            doc = {}
                            for j in range(0, len(item)):
                                doc[column[j]] = item[j]

                            itemList.append(doc)
                    query = {'$or':itemList}

            else:
                query = None


            return query

        def getDataGroupByColumn(self, column, value, attributes, sort):

            if type(value) is list:

                dataList = []

                for item in value:

                    doc = {}
                    doc[column] = item

                    doc['data'] = self.getDataByColumnValue(column, item, attributes, sort)

                    dataList.append(doc)


                return dataList

            else:
                return self.getDataByColumnValue(column, value, attributes, sort)


        def getDataByColumnValue(self, column, value, attributes, sort):

            query = self.getQueryString(column, value)

            projection = {}
            sort_query = []

            print "\nGetting data from MongoDB Group By Column"
            print "Query:" + str(query)

            for attribute in attributes:
                projection[attribute] = 'true'
            #.sort(sort)[('numline',pymongo.ASCENDING),('filepath',pymongo.DESCENDING)]

            for attribute in sort:
                sort_query.append((attribute,pymongo.ASCENDING))

            print "PROJ - "+ str(projection)
            print "SORT - " + str(sort_query)

            try:
                self.connect()
                cursor = self.collection_input.find(query,projection).sort(sort_query)
                return DataObject(self.type, cursor, self.config)

            except Exception as e:
                print "Unexpected error:", type(e), e

        def getDataAll(self, attributes, sort):

            query = {}

            projection = {}
            sort_query = []

            print "Getting data from MongoDB Group By Column"
            print "Query:" + str(query)

            for attribute in attributes:
                projection[attribute] = 'true'
            #.sort(sort)[('numline',pymongo.ASCENDING),('filepath',pymongo.DESCENDING)]

            for attribute in sort:
                sort_query.append((attribute,pymongo.ASCENDING))

            print projection

            try:
                self.connect()
                cursor = self.collection_input.find(query,projection,)
                return DataObject(self.type, cursor, self.config)
            except Exception as e:
                print "Unexpected error:", type(e), e


        def getDataGroupByFilename(self,filename):
            pass

        def saveData(self, data, filename = None, numline = 1):
            try:
                #docs = []
                #numline = 1
                self.connect(OrderedDict)

                print "Saving data"
                print "file "+filename

                # define _id according to position
                for doc in data:

                    orderedDoc = OrderedDict(doc)

                    if filename:
                        orderedDoc["_id"] = OrderedDict((('filepath',filename),('numline',numline)))
                    elif hasattr(self.config, 'OUTPUT_FILE'):
                        orderedDoc["_id"] = OrderedDict((('filepath',self.config.OUTPUT_FILE),('numline',numline)))


                    #print doc
                    #docs.append(doc)
                    self.collection_output.insert_one(orderedDoc)
                    numline += 1
                return True

            except Exception as e:
                print "Unexpected error:", type(e), e
                return False
