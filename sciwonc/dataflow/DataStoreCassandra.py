#!/usr/bin/env python
"""
This is the concrete factory to manage cassandra servers

"""
from DataStoreFactory import DataStoreFactory
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from ConditionTree import ConditionTree
import operator

class DataStoreCassandra(DataStoreFactory):
    """Concrete Factory"""

    class AbstractDataStore(DataStoreFactory.AbstractDataStore):
        """Concrete Product"""

        config = None
        session = None

        collection_input = None
        collection_output = None

        def __init__(self,config):
            try:
                print "Init Cassandra"
                self.config = config
            except:
                print "Error: Config file not found"


        def connection(self):
            print "I am a Cassandra connection"

            if self.config.HOST == "":
                cluster = Cluster()
            else:
                cluster = Cluster(self.config.HOST.split(","))

            keyspace = self.config.DATABASE
            self.session = cluster.connect(keyspace)

            if(hasattr(self.config, 'COLLECTION_INPUT')):
                self.collection_input = self.config.COLLECTION_INPUT

            if(hasattr(self.config, 'COLLECTION_OUTPUT')):
                self.collection_output = self.config.COLLECTION_OUTPUT

            self.session.row_factory = dict_factory;

        def getDataByUnit(self, first, last, attributes, sort):
            keys = first.keys()
            # assumes first and last have two fields
            if (first[keys[-1]] == last[keys[-1]]):
                condition_tree = ConditionTree(ConditionTree(keys[-1], "=", first[keys[-1]]), "and", ConditionTree(ConditionTree(keys[0], ">=", first[keys[0]]), 
                    "and", ConditionTree(keys[0], "<=", last[keys[0]])))
                try:
                    self.connection()
                    return self.select_objects (self.collection_input, condition_tree, attributes, sort)
                except Exception as e:
                    print "Unexpected error:", type(e), e
            else:
                condition_tree1 = ConditionTree(ConditionTree(keys[-1], "=", first[keys[-1]]), "and", ConditionTree(keys[0], ">=", first[keys[0]]))
                condition_tree2 = ConditionTree(ConditionTree(keys[-1], "=", last[keys[-1]]), "and", ConditionTree(keys[0], "<=", last[keys[0]]))
                try:
                    self.connection()
                    objects1 = list(self.select_objects (self.collection_input, condition_tree1, attributes, sort))
                    objects2 = list(self.select_objects (self.collection_input, condition_tree2, attributes, sort))
                    return objects1 + objects2
                except Exception as e:
                    print "Unexpected error:", type(e), e
            

        def columns_values_to_tree(self, columns, values):
            # converts columns = ["event", "task"] and values = [[7, 3], [8, 5]]
            # to CT(CT(CT("event", "=", 7), "and", CT("task", "=", 3)), 'or', CT(CT("event", "=", 8), "and", CT("task", "=", 5)))
            # (CT is ConditionTree)
            if len(columns) == 1 and len(values) == 1:
                return ConditionTree(columns[0], "=", values[0])

            if len(columns) == 2 and len(values) == 1:
                return ConditionTree(ConditionTree(columns[0], "=", values[0][0]), "and",
                    ConditionTree(columns[1], "=", values[0][1]))

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

            condition_tree = self.columns_values_to_tree(column, value)

            try:
                self.connection()
                return self.select_objects(self.collection_input, condition_tree, attributes, sort)
            except Exception as e:
                print "Unexpected error:", type(e), e
            

        def getDataAll(self, attributes, sort):

            try:
                self.connection()
                return self.select_objects(self.collection_input, None, attributes, sort)
            except Exception as e:
                print "Unexpected error:", type(e), e

        def saveData(self, data, filename = None):
            try:
                #docs = []
                numline = 1
                self.connection()

                # define _id according to position
                for doc in data:
                    if "_id" in doc:
                        del doc["_id"]
                    doc["numline"] = str(numline)
                    if filename:
                        doc["filepath"] = filename
                    else:
                        doc["filepath"] = self.config.OUTPUT_FILE
                    self.insert_object(self.collection_output, doc)
                    numline += 1

            except Exception as e:
                print "Unexpected error:", type(e), e


        # helper functions
        def insert_object(self, collection_name, dictionary):

            column_names = "\""
            column_names += "\", \"".join(dictionary.keys()) # string with the column names of the dictionary, separated by commas
            column_names += "\""

            
            for key in dictionary:
                try:
                    a = float(dictionary[key])
                except (ValueError, TypeError):
                    if dictionary[key] is None or dictionary[key] == "":
                        dictionary[key] = "null"
                    else:
                        dictionary[key] = "\'" + dictionary[key] + "\'"
                else:
                    dictionary[key] = str(dictionary[key])

            values = ", ".join(dictionary.values())

            query_string = "INSERT INTO " + collection_name + " (" + column_names + ") VALUES ("+ values +")"
            self.session.execute(query_string)

        def select_objects(self, collection_name, condition_tree = None, attributes = None, order_by = None):
            # there needs to be an index on the condition_tree columns
            if attributes is None:
                projection_string = "*"
            else:
                projection_string = "\""
                projection_string += "\", \"".join(attributes)
                projection_string += "\""
                if "numline" not in attributes:
                    projection_string += ", numline"
                if "filepath" not in attributes:
                    projection_string += ", filepath"
            query_string = "SELECT " + projection_string + " FROM " + collection_name

            if condition_tree is not None:
                query_string += " WHERE " + condition_tree.convert_to_cassandra_condition()

            result = list(self.session.execute(query_string))
            if order_by is not None:
                for i in range(0, len(order_by)):
                    if order_by[i].startswith("_id"):
                        order_by[i] = order_by[i][4:]
                result.sort(key=operator.itemgetter(*order_by))
            return result