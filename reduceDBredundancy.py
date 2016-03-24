"""
Python Script to iterate through Tables in SQLite Database and replace redundant 
text descriptions by numerical codes. 
Store codes and corresponding description in a separate (smaller) table. 

The command line is as follows:
python.exe filename.py database.sqlite output.txt
output is to a specified output file, or to standard out if no file is specified.
Janos Perge, 2016-03-11
"""

import sys
import sqlite3 as sqlite

tablesToIgnore = ["sqlite_sequence"] #list of table names to ignore
columnNames    = ['gcs_q1', 'gcs_q2', 'gcs_q3','rraq', 'ageu', 'gender', 'race1', 'race2', 'ethnic', 'protdev', 'airbag', 'childres', 'trantype', 'tmode', 'dxtype','region1', 'region2', 'region3', \
                  'workrel', 'industry', 'occupation', 'location', 'transfer', 'alcohol', 'drug1', 'drug2', 'eddisp', 'signsoflife', 'payment', 'hospdisp']

outputFilename = 'summary.txt' #give it a filename.txt to suppress display and save as a text file

def Print(msg):
    
    if (outputFilename != None):
        outputFile = open(outputFilename,'a')
        print >> outputFile, msg
        outputFile.close()
    else:
        print msg
        

def dbWrapper(dbFile):
    conn = sqlite.connect(dbFile)
    cur = conn.cursor()
        
    # Get List of Tables:      
    tableListQuery = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY Name"
    cur.execute(tableListQuery)
    tables = map(lambda t: t[0], cur.fetchall())
    
    for table in tables:
        
        #print table        
        
        if (table in tablesToIgnore):
            continue            
            
        columnsQuery = "PRAGMA table_info(%s)" % table
        #cur.execute("PRAGMA table_info(?)" ,(table,))
        cur.execute(columnsQuery)
        colInfo = cur.fetchall()
        columns = map(lambda x: x[1], colInfo)
        
        for column in columns:
            #print table + ' ' + column
            if column in columnNames:
                print 'modifying ' + table + ' ' + column
                
                sql = 'DROP TABLE if exists sub_%s;' % column
                cur.execute(sql)
                conn.commit()
                
                sql = 'CREATE TABLE sub_%s ( %s_id integer primary key, %sdes varchar);' % (column, column, column)
                cur.execute(sql)                
                conn.commit()
    
                sql = 'DROP TABLE if exists temp1'
                cur.execute(sql)
                conn.commit()
                
                sql = 'CREATE TABLE temp1 AS SELECT DISTINCT %s FROM %s;' % (column, table)
                cur.execute(sql)                
                conn.commit()
                
                sql = 'INSERT INTO sub_%s(%sdes) SELECT %s FROM temp1;' % (column, column, column)
                cur.execute(sql)                
                conn.commit()
                
                sql = 'DROP TABLE temp1'
                cur.execute(sql)
                conn.commit()
                
                sql = 'UPDATE %s SET %s = (SELECT sub_%s.%s_id FROM sub_%s WHERE sub_%s.%sdes = %s.%s )' % (table, column, column, column, column, column, column, table, column)
                cur.execute(sql)
                conn.commit()   

    cur.execute('vacuum')
    conn.commit()   
    cur.close()
    conn.close()   

    
            
if __name__ == "__main__":
    if (len(sys.argv) == 2):
        dbFile = sys.argv[1]
        dbWrapper(dbFile)
    elif (len(sys.argv) == 3):
        dbFile = sys.argv[1]
        outputFilename = sys.argv[2]
        dbWrapper(dbFile)
    else:        
        print "\n\tUsage:"
        print "\n\t\tDBDescribe.py {dbFile}"
        print "\t\t\tPrints summary of {dbFile} to standard output."    
        print "\n\t\tDBDescribe.py {dbFile} {outputFile}"
        print "\t\t\tAppends summary of {dbFile} to {outputFile}."    
        

        
        