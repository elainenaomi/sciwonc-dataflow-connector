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
        db = None
        collection_input = None
        collection_output = None
        type = "postgres"

        def __init__(self,config):
            try:
                print "Init Postgres"
                self.config = config
            except:
                print "Error: Config file not found"



        def connection(self):
            print "I am a Postgres Connection"
            strConnection = "host='"+self.config.HOST+"' dbname='"+self.config.DATABASE+"' user='"+self.config.USER+"' password='"+self.config.PASSWORD+"'"
            print strConnection
            self.connection = psycopg2.connect(strConnection)
            self.cursor = self.connection.cursor()

            if (hasattr(self.config, 'COLLECTION_INPUT')):
                self.collection_input = self.config.COLLECTION_INPUT

            if (hasattr(self.config, 'COLLECTION_OUTPUT')):
                self.collection_output = self.config.COLLECTION_OUTPUT


        def getDataByUnit(self, first, last, attributes,sort):

            print "Getting data from Postgres"

            try:
                self.connection()

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
            if type(column) is not list:
                column = [column]

            if type(value) is not list:
                value = [int(value)]

            conditionTree = self.getColumnValuesToTree(column, value)


            try:
                self.connection()

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


                self.cursor.execute(query)

                return DataObject(self.type, self.cursor, self.config)

            except Exception as e:
                print "Unexpected error:", type(e), e

        def getDataAll(self, attributes, sort):

            print "Getting data from Postgres - All"

            try:
                self.connection()

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

        def saveData(self, data, filename = None):
            try:
                #docs = []
                numline = 1
                self.connection()

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
