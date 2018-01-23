#!/usr/bin/python
# -*- coding:utf-8 -*- 
import obsPyCmd
import hashlib
import random
import logging
import re
import time
import os
import threading

logFile = 'log/checkData.log'
if not os.path.exists('log'): os.mkdir('log')
if os.path.exists(logFile) and os.path.getsize(logFile) > 104857600: os.remove(logFile)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(thread)d %(filename)s:%(lineno)d %(levelname)s %(message)s', filename=logFile, filemode='a')

MD5_Global = None

def getAllBucketsFromXML(xmlBody):
    return sorted(re.findall('<Name>(.+?)</Name>', xmlBody))

#返回列表
def getAllObjectsFromXML(xmlBody):
    keys = re.findall('<Key>(.+?)</Key>', xmlBody)
    versions = re.findall('<VersionId>(.+?)</VersionId>', xmlBody)
    for i in range(len(versions)): 
        if versions[i] == 'null': versions[i]=None
    if len(versions)>0 and len(versions) != len(keys):
        logging.error('response error, versions != keys %s' %xmlBody)
        return []
    if not len(versions): versions = [None for i in range(len(keys))]
    return zip(keys,versions)

def getMarkerFromXML(xmlBody, markerStr):        
    marker = re.findall('<' + markerStr + '>(.+?)</' + markerStr + '>', xmlBody)
    if marker and marker[0]:
        logging.info('get marker in response %s' %marker[0]) 
        return marker[0]
    else: 
        logging.info('get no marker in response') 
        return None
                     
#若calMd5为True,返回body MD5，否则返回响应body内容。
#若响应错误，返回空。

def make_request(obsRequesthandler, calMd5 = None, process=None):
    global MD5_Global
    myHTTPConnection = obsRequesthandler.myHTTPConnection
    obsRequest =  obsRequesthandler.obsRequest
    
    returnData = None
    
    #如果计算MD5则随机一个CHUNK_SIZE,否则固定CHUNK_SIZE大小。
    if calMd5: 
        md5hashPart = 0; md5hashTotal = 0; fileHash = hashlib.md5();
        checkData = False
        CHUNK_SIZE = random.randint(4096,1048576)
        logging.debug('CHUNK_SIZE: %d' %CHUNK_SIZE)
    else: CHUNK_SIZE = 65536
    peerAddr = myHTTPConnection.host; localAddr = ''
    httpResponse = None
    recvBody = ''
    start_time = time.time()
    end_time=0; status = '9999 '
    try:
        start_time = time.time()
        myHTTPConnection.connection.putrequest(obsRequest.method, obsRequest.url, skip_host=1)
        #发送HTTP头域 
        for k in obsRequest.headers.keys():
            myHTTPConnection.connection.putheader(k, obsRequest.headers[k])
        myHTTPConnection.connection.endheaders()
        localAddr = str(myHTTPConnection.connection.sock._sock.getsockname())
        peerAddr = str(myHTTPConnection.connection.sock._sock.getpeername())
        logging.debug( 'Request:[%s], conn:[%s->%s], sendURL:[%s], sendHeaders:[%r], sendContent:[%s]' \
                       %(obsRequest.requestType, localAddr, peerAddr, obsRequest.url, obsRequest.headers, obsRequest.sendContent[0:1024]))
        myHTTPConnection.connection.send(obsRequest.sendContent)
        waitResponseTimeStart = time.time()
        #接收响应
        httpResponse = myHTTPConnection.connection.getresponse(buffering=True)
        waitResponseTime = time.time() - waitResponseTimeStart
        logging.debug('get response, wait time %.3f' %waitResponseTime)
        #读取响应体
        contentLength  = int(httpResponse.getheader('Content-Length', '-1'))
        logging.debug('get ContentLength: %d' %contentLength)
        
        #区分不同的请求，对于成功响应的GetObject请求，需要特殊处理,否则一次读完body内容。
        #需要考虑range下载，返回2xx均为正常请求。
        recvBytes = 0 
        if (httpResponse.status < 300) and obsRequest.requestType in ('GetObject'):
            #同时满足条件，才校验数据内容。
            #1.打开calMd5开关。2.GetObject操作；3.正确返回200响应(206不计算）
            while True:
                datatmp = httpResponse.read(CHUNK_SIZE)
                if not datatmp: break
                recvBytes += len(datatmp)
                if calMd5:
                    lastDatatmp = datatmp
                    fileHash.update(datatmp)
            recvBody = '[receive content], length: %d' %recvBytes
            if calMd5: 
                md5hashTotal = fileHash.hexdigest( )
                returnData = md5hashTotal
            else:
                returnData = recvBody
                
        else:
            returnData = httpResponse.read()
            recvBytes = len(returnData)
            
        
        #要读完数据才算请求结束
        end_time = time.time()
        status = str(httpResponse.status) + ' ' + httpResponse.reason
        
        #记日志、重定向(<400:debug; >=400,<500: warn; >=500:error)
        if httpResponse.status < 400:
            logging.debug('Request:[%s], conn: [%s->%s], URL:[%s], waitResponseTime:[%.3f], responseStatus:[%s], %r, %r' \
                          %(obsRequest.requestType, localAddr, peerAddr,obsRequest.url, waitResponseTime, status, str(httpResponse.msg), recvBody[0:1024]))
        elif httpResponse.status < 500:
            logging.warn('Request:[%s], conn: [%s->%s], URL:[%s], waitResponseTime:[%.3f], responseStatus:[%s], %r, %r' \
                         %(obsRequest.requestType, localAddr, peerAddr,obsRequest.url,waitResponseTime, status, str(httpResponse.msg), recvBody[0:1024]))
        else:
            logging.error('Request:[%s], conn: [%s->%s], URL:[%s], waitResponseTime: [%.3f], responseStatus:[%s], %r, %r' \
                          %(obsRequest.requestType, localAddr, peerAddr,obsRequest.url, waitResponseTime, status, str(httpResponse.msg), recvBody[0:1024]))
            if (httpResponse.status == 503):
                flowControllMsg = 'Service unavailable, local data center is busy'
                if recvBody.find(flowControllMsg) != -1: status = '503 Flow Control' #标记外部流控
        requestID = httpResponse.getheader('x-amz-request-id', '9999999999999998')
        #部分错误结果的头域中没有包含x-amz-request-id,则从recvBody中获取
        if requestID == '9999999999999998' and httpResponse.status >= 300:
            requestID = _getRequestIDFromBody_(recvBody)
        if obsRequest.method != 'HEAD' and contentLength != -1 and contentLength != recvBytes:
            logging.error('data error. contentlength %d != dataRecvSize %d' %(contentLength, recvBytes))
            raise Exception("Data Error Content-Length")
    except KeyboardInterrupt:
        if not status: status = '9991 KeyboardInterrupt'
    except Exception, data:
        returnData = None
        import traceback
        stack = traceback.format_exc()
        logging.error('Caught exception:%s, Request:[%s], conn: [local:%s->peer:%s], URL:[%s], responseStatus:[%s], responseBody:[%r]' \
                      %(data, obsRequest.requestType, localAddr, peerAddr, obsRequest.url, status, recvBody[0:1024]))
        logging.error('print stack: %s' %stack)
        print 'ERROR: request %s/%s except: %s' %(obsRequest.bucket, obsRequest.key, stack)
    finally:
        if not end_time: end_time = time.time()
        #关闭连接：1.按服务端语义，若connection:close，则关闭连接。
        if httpResponse and (httpResponse.getheader('connection', '').lower() == 'close' or httpResponse.getheader('Connection', '').lower() == 'close'):
             #关闭连接，让后续请求再新建连接。
             logging.info('server inform to close connection')
             myHTTPConnection.closeConnection()
        #2.客户端感知的连接类错误，关闭连接。
        elif not status <= '600':
            logging.warning('caught exception, close connection')
            #很可能是网络异常，关闭连接，让后续请求再新建连接。
            myHTTPConnection.closeConnection()
            time.sleep(.1)
        #3.客户端配置了短连接
        elif not myHTTPConnection.longConnection:
            #python 2.7以下存在bug，不能直接使用close()方法关闭连接，不然客户端存在CLOSE_WAIT状态。
            if myHTTPConnection.isSecure:
                try:
                    import sys
                    if sys.version < '2.7':
                        import gc
                        gc.collect(0)
                except: pass
            else: myHTTPConnection.closeConnection()
        if process: MD5_Global = returnData
        return returnData




if __name__ == '__main__':
    global MD5_Global_
    printResult = time.time()
    Service_1= '100.61.5.3'
    Service_2 = '100.61.5.13'
    
    
    #可以指定多个用户的AK,SK
    User_AKSK = ['UDSIAMSTUBTEST000101,Udsiamstubtest000000UDSIAMSTUBTEST000101',]
    
    #server = '127.0.0.1', isSecure = False, timeout=80, serialNo = None, longConnection = False
    server1_conn = obsPyCmd.MyHTTPConnection(host=Service_1, isSecure=False, timeout=600, serialNo=0, longConnection=False)
    server2_conn = obsPyCmd.MyHTTPConnection(host=Service_2, isSecure=False, timeout=600, serialNo=0, longConnection=False)
    
    totalObjectsOK = 0
    totalObjectsErr = 0
    totalReadErr = 0
    userOK=True
    for AKSK in User_AKSK:
        print 'INFO: compare user %s' %AKSK
        #列举用户所有桶
        obsRequest = obsPyCmd.OBSRequestDescriptor(requestType ='ListUserBuckets', ak = AKSK.split(',')[0], sk = AKSK.split(',')[1], \
                                                   AuthAlgorithm='AWSV2', virtualHost = False, domainName = '', region='')
        obsRequesthandler1 =  obsPyCmd.OBSRequestHandler(obsRequest, server1_conn)
        Buckets_1 = make_request(obsRequesthandler1)
        obsRequesthandler2 =  obsPyCmd.OBSRequestHandler(obsRequest, server2_conn)
        Buckets_2 = make_request(obsRequesthandler2)
        
        #比较桶是否一致
        Buckets_1 = getAllBucketsFromXML(Buckets_1)
        Buckets_2 = getAllBucketsFromXML(Buckets_2)
        logging.info('Buckets_1: %r, Buckets_2: %r' %(Buckets_1, Buckets_2))
        print 'Buckets on Server1: %r, Buckets on Server2: %r' %(Buckets_1, Buckets_2)
        Buckets = set(Buckets_1) & set(Buckets_2)
        if not Buckets:
            logging.error('find no same buckets exit')
            print 'ERROR: no same buckets for this user'
            break
        
        open('Objects_1_List.txt','w').write('')
        open('Objects_2_List.txt','w').write('')
        #遍历桶
        for bucket in Buckets:
            open('Objects_1_List.txt','a').write('\n' + bucket)
            open('Objects_2_List.txt','a').write('\n' + bucket)
            msg = 'INFO: compare bucket: %s' %bucket
            logging.info(msg)
            print msg
            obsRequest = obsPyCmd.OBSRequestDescriptor(requestType ='ListObjectsInBucket', ak = AKSK.split(',')[0], sk = AKSK.split(',')[1], \
                                                       AuthAlgorithm='AWSV2', virtualHost =False, domainName = '', region='')
            obsRequest.queryArgs['max-keys'] = '999'
            obsRequest.queryArgs['versions'] = None
            obsRequest.bucket = bucket
            Objects_1_List = []; Objects_2_List = []
            k_marker1 = ''; k_marker2=''
            v_marker1 = ''; v_marker2=''
            while k_marker1 != None or k_marker2 != None:
                if k_marker1 != None:
                    if k_marker1: obsRequest.queryArgs['key-marker'] =  k_marker1
                    if v_marker1: obsRequest.queryArgs['version-id-marker'] =  v_marker1
                    obsRequesthandler1 =  obsPyCmd.OBSRequestHandler(obsRequest, server1_conn)
                    Objects_1 = make_request(obsRequesthandler1)
                    k_marker1 = getMarkerFromXML(Objects_1, 'NextKeyMarker')
                    v_marker1 = getMarkerFromXML(Objects_1, 'NextVersionIdMarker')
                    if v_marker1 == 'null': v_marker1 = None
                    newObjs1 = getAllObjectsFromXML(Objects_1)
                    Objects_1_List += newObjs1
                    logging.debug('Objects_1_List: %s' %Objects_1_List)
                    open('Objects_1_List.txt','a').write('\n\t' + str(newObjs1).replace('), (', '\n\t'))
                if k_marker2 != None:
                    if k_marker2: obsRequest.queryArgs['key-marker'] =  k_marker2
                    if v_marker2: obsRequest.queryArgs['version-id-marker'] =  v_marker2
                    obsRequesthandler2 =  obsPyCmd.OBSRequestHandler(obsRequest, server2_conn)
                    Objects_2 = make_request(obsRequesthandler2)
                    k_marker2 = getMarkerFromXML(Objects_2, 'NextKeyMarker')
                    v_marker2 = getMarkerFromXML(Objects_2, 'NextVersionIdMarker')
                    if v_marker2 == 'null': v_marker2 = None
                    newObjs2 = getAllObjectsFromXML(Objects_2)
                    Objects_2_List += newObjs2
                    logging.debug('Objects_2_List: %s' %Objects_2_List)
                    open('Objects_2_List.txt','a').write('\n\t' + str(newObjs2).replace('), (', '\n\t'))
                    
                
                #找到合集中相同集合
                Obj12 = set(Objects_1_List) & set(Objects_2_List)
                logging.info('get same objects %d, len Obj1:%d, lenObj2:%d' %(len(Obj12),len(Objects_1_List), len(Objects_2_List)))
                #校验obj
                for obj in Obj12:
                    #2边读对象
                    msg = 'INFO: compare object: %s/%s' %(bucket,obj)
                    #print msg
                    logging.info(msg)
                    
                    obsRequest_getobj = obsPyCmd.OBSRequestDescriptor(requestType ='GetObject', ak = AKSK.split(',')[0], sk = AKSK.split(',')[1], \
                                                                      AuthAlgorithm='AWSV2', virtualHost =False, domainName = '', region='')
                    obsRequest_getobj.bucket = bucket
                    obsRequest_getobj.key = obj[0]
                    if obj[1]: obsRequest_getobj.queryArgs['versionId'] = obj[1]
                    obsRequesthandler1 =  obsPyCmd.OBSRequestHandler(obsRequest_getobj, server1_conn)
                    obsRequesthandler2 =  obsPyCmd.OBSRequestHandler(obsRequest_getobj, server2_conn)

                    t1 = threading.Thread(target=make_request, name='thread1', args=(obsRequesthandler1, True, True))
                    t1.start(); 
                    md5_2 = make_request(obsRequesthandler2, True, False)
                    t1.join(); 
                    md5_1 = MD5_Global
                    if not md5_1 or not md5_2:
                        totalReadErr += 2
                        msg = 'ERROR: read Object error. can not get md5. %s/%s, md5_1:%s, md5_2:%s'  %(bucket, obj, md5_1, md5_2) 
                        print msg; logging.error(msg)
                    elif md5_1 != md5_2:
                        totalObjectsErr += 2
                        msg = 'ERROR: Data Not Consistent. object: [%s/%s], MD5 on server1: %s, MD5 on server2: %s' %(bucket, obj, md5_1, md5_2)
                        print msg
                        logging.error(msg)
                    elif md5_1 == md5_2:
                        totalObjectsOK += 2
                        logging.info('Data Consistent. object: [%s/%s], MD5 on server1: %s, MD5 on server2: %s' %(bucket, obj, md5_1, md5_2))
                       
                    if time.time() - printResult > 10: 
                        progress = 'INFO: totalObjectsOK: %d, totalObjectsErr:%d, totalReadErr:%d' %(totalObjectsOK, totalObjectsErr,totalReadErr)
                        print progress; logging.info(progress)
                        printResult = time.time()

                #去掉各自相同的部分
                Objects_1_List = list(set(Objects_1_List) - Obj12)
                Objects_2_List = list(set(Objects_2_List) - Obj12)
                #如果不相同的部分相差超过了10000个，跳过该桶
                if len(Objects_1_List)>10000 or len(Objects_2_List) >10000:
                    msg = 'ERROR: too many objects not equal, jump this bucket...'
                    totalObjectsErr += 10000
                    logging.error(msg); print msg; 
                    break

            if Objects_1_List:
                totalObjectsErr += len(Objects_1_List)
                msg = 'ERROR: Objects in server1 but not in server2 %r' %Objects_1_List
                print msg 
                logging.error(msg)
            if Objects_2_List:
                totalObjectsErr += len(Objects_2_List)
                msg = 'ERROR: Objects in server2 but not in server1 %r' %Objects_2_List
                print msg 
                logging.error(msg)
   
    logging.info('totalObjectsOK: %d, totalObjectsErr:%d, totalReadErr:%d' %(totalObjectsOK, totalObjectsErr,totalReadErr))
    print 'totalObjectsOK: %d, totalObjectsErr:%d, totalReadErr:%d' %(totalObjectsOK, totalObjectsErr,totalReadErr)
