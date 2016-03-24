"""
Add national population estimates from census data broken down to gender,
race, hispanic ethnicity and count. Note that the race column contains an extra
attribute 'two or more races' compared to the NTDB. Also, does not contain 
fields of 'unknown' gender. So part of the NTDB data should be ignored when 
using population estimates
Janos Perge, 2016 March
"""

import sqlite3
import csv
import os
import glob
import sys
 
csvfile = r'C:\Users\Admin\Documents\NationalTrauma\buildDB\jpergeim3exfrc1.asc'
db = r'C:\Users\Admin\Documents\NationalTrauma\buildDB\ixNTDBshort.sqlite'

conn = sqlite3.connect(db)
conn.text_factory = str  # allows utf-8 data to be stored
c = conn.cursor()
       
tablename = 'popnational'
print 'processing ' + tablename + '...'

#%%
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

            #create table without primary or compound keys, and index later
            sql = "CREATE TABLE %s (age integer, sex integer, race6 integer, hisp integer, pop integer)" % (tablename,)
            c.execute(sql)

            #table with multiple entries per age: add index to table on a specific column name
            index = "%s_age" % ( tablename, )
            sql = "CREATE INDEX %s on %s (age)" % ( index, tablename)
            c.execute(sql)

            rowlen = len(row)
        
        else:
        # skip lines that don't have the right number of columns
            if len(row) == rowlen:
                # insert data from current column and current row into current table
                insertsql = "INSERT INTO %s (age, sex, race6, hisp, pop) VALUES (%s)" % (tablename, ", ".join([row[1], row[2], row[3], row[4], row[6]]))
                c.execute(insertsql)					
            else:
                print 'row number ' + str(tel) + ' in ' + tablename + ' is shorter and deleted'
                tel += 1
                
        conn.commit()
		
 
c.close()
conn.close()

print '... finished'
