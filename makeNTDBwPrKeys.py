# makeNTDBsqlite.py: creates SQLite database from CSV files - customized for NTDB db with module imported to define correct data types
# syntax: python filename.py nameofdb.sqlite directory
# Original code modified from csv2sqlite.py
# Modified by Janos Perge. Updated indexing, 2016 March

import sqlite3
import csv
import os
import glob
import sys

# import dictionary 'datatype'
from NTDBdatatype import datatype

# manually selected list of tables where primary keys or non-unique indices should be set to a given attribute:
# create dictionary assigning primary keys to data tables
primaryKeyDict = {}
indexDict      = {}

#tables with a unique primary key:
primaryKeyDict['RDS_demo'] = 'inc_key'
primaryKeyDict['RDS_ecode'] = 'inc_key'
primaryKeyDict['RDS_ed'] = 'inc_key'
primaryKeyDict['RDS_discharge'] = 'inc_key'
primaryKeyDict['RDS_pcodedes'] = 'pcode'
primaryKeyDict['RDS_facility'] = 'fac_key'

#tables with compound keys
primaryKeyDict['RDS_vitals'] = 'inc_key, vstype'
primaryKeyDict['RDS_comorbid'] = 'inc_key, comorkey'
primaryKeyDict['RDS_complic'] = 'inc_key, complkey'

#tables with multiple entries and column combinations are indexed to inc_key or other keys
indexDict['RDS_dcode'] = 'inc_key'
indexDict['RDS_dcodedes'] = 'dcode'
indexDict['RDS_pcode'] = 'inc_key'
indexDict['RDS_ecodedes'] = 'ecode'

indexDict['RDS_aispcode'] = 'inc_key'
indexDict['RDS_ais98pcode'] = 'inc_key'
indexDict['RDS_aisccode'] = 'inc_key'
indexDict['RDS_protdev'] = 'inc_key'
indexDict['RDS_transport'] = 'inc_key'
indexDict['RDS_transport'] = 'inc_key'


db = sys.argv[1]
 
conn = sqlite3.connect(db)
conn.text_factory = str  # allows utf-8 data to be stored
 
c = conn.cursor()
 
# traverse the directory and process each .csv file
for csvfile in glob.glob(os.path.join(sys.argv[2], "*.csv")):
        
    # remove the path and extension and use what's left as a table name
    tablename = os.path.splitext(os.path.basename(csvfile))[0]
    print 'processing ' + tablename + '...'

    # open current file using csv module 
    with open(csvfile, "rb") as f:
        reader = csv.reader(f)
 
        header = True
        tel = 0
        for row in reader:
            if header:

                # gather column names from the first row of the csv
                header = False
 
                # clear any existing table with the current name
                sql = "DROP TABLE IF EXISTS %s" % tablename
                c.execute(sql)

                # create table with current column from current file, with correct datatype defined by NTDBdatatype module
                if tablename in primaryKeyDict:
                    #include primary or compound keys as deterimed by primaryKeyDict
                    sql = "CREATE TABLE %s (%s, PRIMARY KEY (%s))" % (tablename,
                              ", ".join([ "%s %s" % ( column.lower(), datatype[column] ) for column in row ]), primaryKeyDict[tablename])
                    c.execute(sql)
                else:
                    #create table without primary or compound keys, and index later
                    sql = "CREATE TABLE %s (%s)" % (tablename,
                              ", ".join([ "%s %s" % ( column.lower(), datatype[column] ) for column in row ]))
                    c.execute(sql)
 
                if tablename in indexDict:
                    #tables with multiple entries per incident: add index to table on a column name specified in indexDict (indexing will improve performance)
                    index = "%s__%s" % ( tablename, indexDict[tablename] )
                    sql = "CREATE INDEX %s on %s (%s)" % ( index, tablename, indexDict[tablename] )
                    c.execute(sql)
 
                # insert data from current column and current row into current table
                insertsql = "INSERT INTO %s VALUES (%s)" % (tablename,
                            ", ".join([ "?" for column in row ]))
 
                rowlen = len(row)
            else:
                # skip lines that don't have the right number of columns
                if len(row) == rowlen:
					c.execute(insertsql, row)					
                else:
					print 'row number ' + str(tel) + ' in ' + tablename + ' is shorter and deleted'
            tel += 1
			
        conn.commit()
		
 
c.close()
conn.close()
