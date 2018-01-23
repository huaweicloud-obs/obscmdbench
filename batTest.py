#!/usr/bin/python
from run import testCaseNames
import os, os.path
import time

#grep -v '200 OK' result/*detail*.csv | grep -v Operation | grep -v '204 N'

#testCaseNames = {100:'ListUserBuckets', 
#                 101:'CreateBucket', 
#                 102:'ListObjectsInBucket', 
#                 103:'HeadBucket', 
#                 104:'DeleteBucket', 
#                 105:'BucketDelete',
#                 106:'OPTIONSBucket',
#                 141:'PutBucketWebsite',
#                 142:'GetBucketWebsite',
#                 143:'DeleteBucketWebsite',
#                 151:'PutBucketCORS',
#                 152:'GetBucketCORS',
#                 153:'DeleteBucketCORS',
#                 201:'PutObject',
#                 202:'GetObject',
#                 203:'HeadObject',
#                 204:'DeleteObject',
#                 205:'DeleteMultiObjects',
#                 206:'CopyObject',
#                 211:'InitMultiUpload',
#                 212:'UploadPart',
#                 213:'CopyPart',
#                 214:'CompleteMultiUpload',
#                 900:'MixOperation'
#                 }  
def modifyFile(file, item, value):
    lines = open(file, 'rb').readlines()
    newLines = []
    getItem = False
    for line in lines: 
        if not line.startswith('#') and '=' in line: 
            if str(item) == line.split('=')[0].strip(): 
                newLine = line.split('=')[0] + ' = ' + str(value)
                print 'modify %s = %s' %(item, value)
                getItem = True
            else: newLine = line
            newLines.append(newLine)
    if getItem:            
        open(file, 'wb').write('\n'.join(newLines))
    else: print 'can not find item %s in file %s' %(item, value)
    
runSeq = [101, 103, 141, 142, 151, 152, 201, 202, 203, 206, 205, 211, 212, 213, 214, 204, 153, 143, 105, 101, 104, 900]

#bak config.dat
configBak = 'config.dat.%d' %int(time.time())
 
try:
    open(configBak, 'wb').write(open('config.dat', 'rb').read())
    modifyFile(configBak, 'ThreadsPerUser', '2')
    modifyFile(configBak, 'RequestsPerThread', '3')
    modifyFile(configBak, 'BucketsPerUser', '3')
    modifyFile(configBak, 'BucketNamePrefix', int(time.time()))
    modifyFile(configBak, 'Max-keys', random.randint(1,10))
    modifyFile(configBak, 'ObjectNamePrefix', int(time.time()))
    modifyFile(configBak, 'ObjectSize', random.randint(1,4097))
    modifyFile(configBak, 'ObjectsPerBucketPerThread', random.randint(1,10))
    modifyFile(configBak, 'ObjectLexical','True')
    modifyFile(configBak, 'DeleteObjectsPerRequest','3')
    modifyFile(configBak, 'PartsForEachUploadID','3')
    
    
 
except Exception, data:
    print 'prepare config file except: %s' %data

for testCase in  runSeq:
    print 'run ', testCase, testCaseNames[testCase]
    os.system('./run.py %d 2 %s' %(testCase, configBak))
    
