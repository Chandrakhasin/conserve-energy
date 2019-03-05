#Actual log parser u_ex180414.log full parse
import re
import os
import functools
import operator
import sys
import cx_Oracle #pip install cx_oracle


#Read a file and parse it(convert it to csv)
logPath = os.path.join("C:\\","home","harish","Desktop","u_ex180414.log")
f = open(logPath,'r') #will we get stackoverflow error if file is big?

def parseMeterDat(siteId,dt,record):
  compRec = siteId+","+dt+","+record[:-1]
  recArr = compRec.split(',')
  return recArr

  
def parseCsUriQueryDat(siteId,dt,queryDat):
  queryDatArr=queryDat.split(':')
  finalRecList = [parseMeterDat(siteId,dt,rec) for rec in queryDatArr]
  return finalRecList
  
def parseData(lineStr):
  csUriQueryPattern = '.*id=(.*)&dt=(.*)&dat=:(.*,)'
  qre = re.search(csUriQueryPattern,lineStr)
  #cs-uri-query
  return parseCsUriQueryDat(qre.group(1),qre.group(2),qre.group(3))
  
parsedList = [parseData(line) for line in f if 'GET' and 'id=' in line] 
#https://stackoverflow.com/questions/952914/how-to-make-a-flat-list-out-of-list-of-lists
parsedFlattenedList = functools.reduce(operator.iconcat, parsedList, []) 
f.close()

#oracle
#username - user
#password - password
db = cx_Oracle.connect('user/password@localhost:1521/XE')
cursor=db.cursor()
print(db.version)

cursor.prepare("INSERT INTO ENERGYTAB(SITEID,DateTime,METERID,EBENERGY,DGENERGY,VOLTAGE,CURRENTEN,ACTPOWER,APPPOWER,POWERFACTOR,MAXDEMAND,FLAG) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12)")
cursor.executemany(None, parsedFlattenedList)
db.commit()
r = cursor.execute("SELECT COUNT(*) FROM ENERGYTAB")
print(f'inserted {cursor.fetchone()} rows')

#SITEID,DateTime,METERID,EBENERGY,DGENERGY,VOLTAGE,CURRENT,ACTPOWER,APPPOWER,POWERFACTOR,MAXDEMAND,FLAG
#for row in cursor.execute("SELECT * FROM ENERGYTAB"):
#    print(row)
print("Successfully completed parsing log file and loading it into database.")

#References:
#https://www.oracle.com/technetwork/articles/dsl/prez-python-queries-101587.html