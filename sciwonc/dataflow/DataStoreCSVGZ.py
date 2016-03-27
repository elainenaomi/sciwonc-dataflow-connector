"""
This is the concrete factory to manage mongodb servers

"""
from DataStoreFactory import DataStoreFactory
from DataFileCSVGZ import DataFileCSVGZ
from DataObject import DataObject
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

        def readFiles(self):
            self.files = []

            for f in self.config.FILENAME:
                self.files.append(DataFileCSVGZ(self.config,f))

            return self.files

        def getDataByUnit(self, first, last, attributes,sort):
            return self.readFiles()

        def getDataGroupByColumn(self, column, value, attributes, sort):
            return self.readFiles()

        def getDataAll(self, attributes, sort):
            return self.readFiles()

        def getDataGroupByFilename(self,filename):
            return self.readFiles()

        def saveData(self, data, filename):


            # write schema in the first line

            # write the data


            # save file


            try:

                if filename:
                    output_filename = filename
                elif hasattr(self.config, 'OUTPUT_FILE'):
                    output_filename = self.config.OUTPUT_FILE

                csvgz_file = gzip.open(output_filename, 'wb')

                writer = csv.writer(csvgz_file, delimiter=self.config.DELIMITER)

                numline = 1

                print "Saving data"
                print "file "+output_filename

                if(self.config.SCHEMA_FIRST_LINE == True):
                    #schema = data[0].keys()
                    schema = data[0].keys()
                    writer.writerow(schema)


                # define _id according to position
                for doc in data:
                    #print doc
                    writer.writerow(doc.values()) #TODO: review the order
                    numline += 1

            except Exception as e:
                print "Unexpected error:", type(e), e
            finally:
                csvgz_file.close()
