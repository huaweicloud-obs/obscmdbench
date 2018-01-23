#!/usr/bin/python
# -*- coding:utf-8 -*- 
import multiprocessing
from datetime import datetime 
from multiprocessing import Process,Value,Lock
from multiprocessing import Queue
import sys
import results
import logging
import time

if __name__ == '__main__':
    
    LogLevel = logging.DEBUG
    LogFile = 'Analysis.log' #logfile path
    logging.basicConfig(level=LogLevel,
                format='%(asctime)s [%(process)d] [%(filename)s:%(lineno)d] [%(levelname)s] %(message)s',
                filename=LogFile,
                filemode='a')
    
    valid_start_time = Value('d',float(sys.maxint))
    valid_end_time = Value('d', float(sys.maxint))
    currentUsers = Value('i', 1)   
    print_lock =  multiprocessing.Lock()
    CONFIG = {'Users':1, 'ThreadsPerUser':1, 'LatencySections': '500,1000,3000,10000', 'StatisticsInterval':60, 'BadRequestCounted': False, 'IsHTTPs': False, 'RecordDetails': False} 
    #results_queue, 请求记录保存队列。多进程公用。
    results_queue = Queue(0)
    
    totalRequest = -1
    
    #读文件
    detailFilePath = ''
    if len(sys.argv[1:]) > 0:
        detailFilePath = sys.argv[1:][0]
    else:
        print 'please specified the detail file path'
        sys.exit() 
    
    #启动统计计算结果的进程 。用于从队列取请求记录，保存到本地，并同时刷新实时结果。
    results_writer = results.ResultWriter(CONFIG, 'Analysis', results_queue, totalRequest,\
                                          valid_start_time, valid_end_time, currentUsers, print_lock)
    results_writer.daemon = True
    results_writer.start()
    import time
    time.sleep(2)
    tup = ()
    try:
        with open(detailFilePath, 'r') as fd:
            lastEndTime = 0
            for line in fd:
                if line.strip() == '':  continue
                #10,update2.account.10,/object2-4,PutObject,1397019357.495558,1397019358.186864,0.691306,5009527,0,MD5:None,DCD2FC98B6F70000014544D7B0D7CF55,200 OK
                #2,id.account.2,/d0ef0c9cac9d1563d3b4.bucket.0/,CreateBucket,1441722306.306353,1441722334.985173,28.678820,0,302,,D8A237462850BFD0E9FC95BC14D146AB,500 Internal Server Error
              #processId, user.username, rest.url, testCaseNames[CONFIG['Testcase']], resp.start_time, resp.end_time, resp.sendBytes, resp.recvBytes, '', resp.requestID, resp.status)
                if line.startswith('ProcessId'): continue
                arr = line.split(',')
                del arr[6]
                tup = tuple(arr)
                if valid_start_time.value >= float(sys.maxint):
                     valid_start_time.value = float(tup[4]) 
                if results_queue.qsize() > 800: 
                    time.sleep(.1)
                results_queue.put(tup)
            fd.close() 
    except Exception, data:
        print "read detail file error %r" %data
        sys.exit()
    finally:
        try:
            valid_end_time.value = tup[5]
        except: pass

#等待result退出        
    tmpi = 0
    while results_writer.is_alive():
        currentUsers.value = -1 
        tmpi += 1
        if tmpi > 300:
            results_writer.terminate()
            time.sleep(1)
            sys.exit()

    
