"""
This is the concrete factory to manage mongodb servers

"""
from DataStoreFactory import DataStoreFactory
import sys, getopt
import os
import os.path
import csv
import gzip

class DataStoreCSVGZ(DataStoreFactory):
    """Concrete Factory"""

    class AbstractDataStore(DataStoreFactory.AbstractDataStore):
        """Concrete Product"""

        config = None

        def __init__(self,config):
            try:
                print "Init CSV GZ"
                self.config = config
            except:
                print "Error: Config file not found"



        def connection(self):
            print "I am a CSV GZ Factory"
            pass

        def getDataByUnit(self, first, last, attributes,sort):
            pass


        def getQueryString(self, column, value):
            pass

        def getDataGroupByColumn(self, column, value, attributes, sort):
            pass

        def getDataAll(self, attributes, sort):
            pass

        def getDataGroupByFilename(self,filename):

            data = []
            schema = self.config.SCHEMA_STRING.split(self.config.DELIMITER)
            for filepath in filename:

                if os.path.isfile(filepath):
                    with gzip.open(filepath, 'rb') as csv_file:
                        reader = csv.reader(csv_file, delimiter=self.config.DELIMITER)
                        line = 1
                        docs = []

                        for row in reader:
                    	    doc = {}

                    	    doc["_id"] = {"filepath": filepath, "numline":line}
                    	    line += 1

                    	    column = 0

                            if(self.config.SCHEMA_FIRST_LINE == True):
                                schema = next(reader)

                    	    for header in schema:
                    		    doc[header] = row[column]
                    		    column += 1

                    	    docs.append(doc);

                        doc = {}
                        doc['filename'] = filepath
                        doc['data'] = docs

                        data.append(doc)

                        print filepath

            print self.config.FILENAME

            return data

        def saveData(self, data, filename):
            pass
