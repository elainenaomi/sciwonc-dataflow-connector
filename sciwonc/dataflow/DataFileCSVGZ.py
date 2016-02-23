import sys, getopt
import os
import csv
import gzip

class DataFileCSVGZ(object):
    def __init__(self, config, filepath):
        self.config = config
        self.filepath = filepath
    def __str__(self):
        return "reading file "+ self.filepath

    def readFile(self):
        print "readFile DataFile"
        if os.path.isfile(self.filepath):
            with gzip.open(self.filepath, 'rb') as csv_file:
                reader = csv.reader(csv_file, delimiter=self.config.DELIMITER)
                line = 1
                docs = []

                if(self.config.SCHEMA_FIRST_LINE == True):
                    schema = next(reader)
                else:
                    schema = self.config.SCHEMA_STRING.split(self.config.DELIMITER)

                for row in reader:
                    doc = {}

                    doc["_id"] = {"filepath": self.filepath, "numline":line}
                    line += 1

                    column = 0



                    for header in schema:
                        doc[header] = row[column]
                        column += 1

                    docs.append(doc);

                data = {}
                data['filename'] =  self.filepath
                data['data'] = docs

                return data

    def getData(self):

        if (self.config.OPERATION_TYPE == "UNIT"):
            return self.readFile()
        elif (self.config.OPERATION_TYPE == "GROUP_BY_COLUMN"):
            return self.getDataGroupByColumn()
        elif (self.config.OPERATION_TYPE == "ALL"):
            return self.readFile()
        elif (self.config.OPERATION_TYPE == "GROUP_BY_FILENAME"):
            return self.readFile()
        else:
            return self.readFile()
    def getDataGroupByColumn(self):
        docs = self.readFile()
        output = {}
        output['filename'] =  self.filepath
        output['data'] = []

        for doc in docs['data']:
            if doc[self.config.COLUMN]:
                if doc[self.config.COLUMN] == self.config.VALUE:
                    output['data'].append(doc)

        return output
