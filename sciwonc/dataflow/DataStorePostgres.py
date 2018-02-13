#!/usr/bin/env python
"""
This is the concrete factory to manage postgres servers

"""
from DataStoreFactory import DataStoreFactory
from ConditionTree import ConditionTree
from DataObject import DataObject

import psycopg2

class DataStorePostgres(DataStoreFactory):
    """Concrete Factory"""

    class AbstractDataStore(DataStoreFactory.AbstractDataStore):
        """Concrete Product"""

        config = None
        connection = None
        cursor = None
        db = None
        collection_input = None
        collection_output = None
        type = "postgres"

        def __init__(self,config):
            try:
                print "Init Postgres"
                self.config = config

                if (hasattr(self.config, 'COLLECTION_INPUT')):
                    self.collection_input = self.config.COLLECTION_INPUT

                if (hasattr(self.config, 'COLLECTION_OUTPUT')):
                    self.collection_output = self.config.COLLECTION_OUTPUT

            except:
                print "Error: Config file not found"



        def connect(self):

            if self.connection is None:
                print "I am a Postgres Connection"
                strConnection = "host='"+self.config.HOST+"' dbname='"+self.config.DATABASE+"' user='"+self.config.USER+"' password='"+self.config.PASSWORD+"'"
                print strConnection
                self.connection = psycopg2.connect(strConnection)
                self.cursor = self.connection.cursor()


        def getDataByUnit(self, first, last, attributes,sort):

            print "Getting data from Postgres"

            try:
                self.connect()

                projection = list()

                for attr in self.config.ATTRIBUTES:
                    projection.append(self.config.PREFIX_COLUMN + attr.replace(" ","_"))

                order = list()

                for attr in self.config.SORT:
                    order.append(self.config.PREFIX_COLUMN + attr.replace(" ","_"))

                query = ' select ' + ",".join(projection)
                query += ' from '+self.collection_input
                query += ' WHERE ('+self.config.PREFIX_COLUMN+'filepath, '+self.config.PREFIX_COLUMN+'numline::int) between '
                query += ' (%s,%s) and '
                query += ' (%s,%s)  '
                query += ' order by '+ ",".join(order)

                values = (self.config.FIRST_ITEM['filepath'],self.config.FIRST_ITEM['numline'],self.config.LAST_ITEM['filepath'],self.config.LAST_ITEM['numline'])

                self.cursor.execute(query, values)

                return DataObject(self.type, self.cursor, self.config)

            except Exception as e:
                print "Unexpected error:", type(e), e

        def getColumnValuesToTree(self, columns, values):

            if len(columns) == 1 and len(values) == 1:
                return ConditionTree(self.config.PREFIX_COLUMN+columns[0].replace(" ","_"), "=", str(values[0]))

            if len(columns) == 2 and len(values) == 1:
                return ConditionTree(ConditionTree(self.config.PREFIX_COLUMN+columns[0].replace(" ","_"), "=", values[0][0]), "and",
                    ConditionTree(self.config.PREFIX_COLUMN+columns[1].replace(" ","_"), "=", str(values[0][1])))

            if len(columns) > 2 and len(values) == 1:
                return ConditionTree(self.columns_values_to_tree(columns[:-1], [values[0][:-1]]), "and",
                    ConditionTree(columns[-1], "=", values[0][-1]))

            if len(values) == 2:
                return ConditionTree(self.columns_values_to_tree(columns, [values[0]]), "or",
                    self.columns_values_to_tree(columns, [values[1]]))

            if len(values) > 2:
                return ConditionTree(self.columns_values_to_tree(columns, values[:-1]), "or",
                    self.columns_values_to_tree(columns, [values[-1]]))


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
            if type(column) is not list:
                column = [column]

            if type(value) is not list:
                try:
                    value_converted = int(value)
                except ValueError:
                    value_converted = str(value)

                value = [value_converted]



            conditionTree = self.getColumnValuesToTree(column, value)


            try:
                self.connect()

                projection = list()

                for attr in self.config.ATTRIBUTES:
                    projection.append(self.config.PREFIX_COLUMN + attr.replace(" ","_"))

                order = list()

                for attr in self.config.SORT:
                    order.append(self.config.PREFIX_COLUMN + attr.replace(" ","_"))

                query = ' select ' + ",".join(projection)
                query += ' from '+self.collection_input
                if conditionTree is not None:
                    query += " where " + conditionTree.convert_to_sql_condition()
                query += ' order by '+ ",".join(order)
                print query
                cursor = self.connection.cursor()
                cursor.execute(query)

                return DataObject(self.type, cursor, self.config)

            except Exception as e:
                print "Unexpected error:", type(e), e


        def getDataAll(self, attributes, sort):

            print "Getting data from Postgres - All"

            try:
                self.connect()

                projection = list()

                for attr in self.config.ATTRIBUTES:
                    projection.append(self.config.PREFIX_COLUMN + attr.replace(" ","_"))

                order = list()

                for attr in self.config.SORT:
                    order.append(self.config.PREFIX_COLUMN + attr.replace(" ","_"))

                query = ' select ' + ",".join(projection)
                query += ' from '+self.collection_input
                query += ' order by '+ ",".join(order)

                self.cursor.execute(query)

                return DataObject(self.type, self.cursor, self.config)

            except Exception as e:
                print "Unexpected error:", type(e), e


        def getDataGroupByFilename(self,filename):
            pass

        def getDataDistinct(self,column):
            print "DISTINCT"
            print column
            try:

                self.connect()

                distinct_column = self.config.PREFIX_COLUMN + column.replace(" ","_")

                order = list()

                # for attr in self.config.SORT:
                #     order.append(self.config.PREFIX_COLUMN + attr.replace(" ","_"))


                query = ' select distinct(' + distinct_column + ')'
                query += ' from '+self.collection_input
                query += ' limit 2'
                # query += ' order by '+ ",".join(order)

                self.cursor.execute(query)

                return DataObject(self.type, self.cursor, self.config)

            except Exception as e:
                print "Unexpected error:", type(e), e

        def getDataGroupByFixedWindow(self, column, value, attributes, sort):
            print "Group By Fixed Window"



            if type(value) is list:

                dataList = []

                for item in value:
                    first = item[0]
                    last = item[1]

                    doc = {}
                    doc[column] = last
                    doc['data'] = self.getDataByInterval(column, first, last, attributes, sort)
                    dataList.append(doc)


                return dataList

            else:
                return self.getDataByInterval(column, first, last, attributes, sort)

            pass

        def getDataByInterval(self, column, first, last, attributes, sort):
            print "get by interval"
            column = self.config.PREFIX_COLUMN + column.replace(" ","_")

            order = list()
            for attr in sort:
                order.append(self.config.PREFIX_COLUMN + attr.replace(" ","_"))

            query = ' select * '
            query += ' from '+self.collection_input
            query += ' WHERE '+ column + ' between '
            query += ' (%s) and '
            query += ' (%s)  '
            query += ' order by '+ ",".join(order)

            projection = {}
            sort_query = []

            print "\nData by Interval"

            # print "Projection: ", projection
            print "Query - ", query
            print first
            print last

            try:
                self.connect()
                values = (first,last)
                self.cursor.execute(query, values)
                return DataObject(self.type, self.cursor, self.config)
            except Exception as e:
                print "Unexpected error:", type(e), e

        def saveData(self, data, filename = None, numline = 1):
            try:
                #docs = []
                #numline = 1
                self.connect()

                print "Saving data"
                print "file "+filename

                # define _id according to position
                for doc in data:

                    doc.pop("_id", None)
                    #del doc['_id'];

                    if filename:
                        doc['numline'] = numline
                        doc['filepath'] = filename
                    elif hasattr(self.config, 'OUTPUT_FILE'):
                        doc['numline'] = numline
                        doc['filepath'] = self.config.OUTPUT_FILE

                    keys = list()
                    values = list()
                    datatypes = list()

                    for k in doc.keys():
                        keys.append(self.config.PREFIX_COLUMN+k.replace(" ","_"))
                        if doc[k] == '':
                            doc[k] = None

                        values.append(doc[k])
                        datatypes.append("%s")

                    stringInsert = "INSERT INTO "+self.collection_output+" ("+','.join(keys)+") VALUES ("+','.join(datatypes)+")"
                    self.cursor.execute(stringInsert, tuple(values))
                    self.connection.commit()
                    numline += 1



                self.cursor.close()
                self.connection.close()

		return True

            except Exception as e:
                print "Unexpected error:", type(e), e
		return False
