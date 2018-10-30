#!/usr/bin/env python
# -*- coding:utf-8 -*- 
import sys
import os
import time
import math
import random
import commands
import logging
import logging.config
import datetime
import hashlib
import base64
import multiprocessing
import results
import Util
import obsPyCmd
import myLib.cloghandler
from StringIO import StringIO
import string
from constant import Mode
from constant import Role
import threading
import urllib

VERSION = '-------------------obscmdbench: v3.1.7, Python: %s-------------------\n' % sys.version.split(' ')[0]
valid_start_time = None


class User:
    doc = """
        This is user class
    """

    def __init__(self, username, ak, sk):
        self.username = username
        self.ak = ak
        self.sk = sk


def read_config(config_file='config.dat'):
    """
    :rtype : None
    :param config_file: string
    """
    try:
        f = open(config_file, 'r')
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            if line and line[0] != '#':
                CONFIG[line[:line.find('=')].strip()] = line[line.find('=') + 1:].strip()
            else:
                continue
        f.close()
        CONFIG['OSCs'] = CONFIG['OSCs'].replace(' ', '').replace(',,', ',')
        if CONFIG['OSCs'][-1:] == ',':
            CONFIG['OSCs'] = CONFIG['OSCs'][:-1]
        if CONFIG['IsHTTPs'].lower() == 'true':
            CONFIG['IsHTTPs'] = True
        else:
            CONFIG['IsHTTPs'] = False
        CONFIG['ConnectTimeout'] = int(CONFIG['ConnectTimeout'])
        if int(CONFIG['ConnectTimeout']) < 5:
            CONFIG['ConnectTimeout'] = 5
        if CONFIG['LongConnection'].lower() == 'true':
            CONFIG['LongConnection'] = True
        else:
            CONFIG['LongConnection'] = False
        if CONFIG['UseDomainName'].lower() == 'true':
            CONFIG['UseDomainName'] = True
            # 如果使用域名，则OSCs为域名
            CONFIG['OSCs'] = CONFIG['DomainName']
        else:
            CONFIG['UseDomainName'] = False
        if CONFIG['VirtualHost'].lower() == 'true':
            CONFIG['VirtualHost'] = True
        else:
            CONFIG['VirtualHost'] = False
        if CONFIG['ObjectLexical'].lower() == 'true':
            CONFIG['ObjectLexical'] = True
        else:
            CONFIG['ObjectLexical'] = False
        if CONFIG['CalHashMD5'].lower() == 'true':
            CONFIG['CalHashMD5'] = True
        else:
            CONFIG['CalHashMD5'] = False
        CONFIG['Testcase'] = int(CONFIG['Testcase'])
        CONFIG['Users'] = int(CONFIG['Users'])
        CONFIG['UserStartIndex'] = int(CONFIG['UserStartIndex'])
        CONFIG['ThreadsPerUser'] = int(CONFIG['ThreadsPerUser'])
        CONFIG['Threads'] = CONFIG['Users'] * CONFIG['ThreadsPerUser']
        CONFIG['RequestsPerThread'] = int(CONFIG['RequestsPerThread'])
        CONFIG['BucketsPerUser'] = int(CONFIG['BucketsPerUser'])
        if CONFIG['copyDstObjFixed'] and '/' not in CONFIG['copyDstObjFixed']:
            CONFIG['copyDstObjFixed'] = ''
        if CONFIG['copySrcObjFixed'] and '/' not in CONFIG['copySrcObjFixed']:
            CONFIG['copySrcObjFixed'] = ''
        CONFIG['ObjectsPerBucketPerThread'] = int(CONFIG['ObjectsPerBucketPerThread'])
        CONFIG['DeleteObjectsPerRequest'] = int(CONFIG['DeleteObjectsPerRequest'])
        CONFIG['PartsForEachUploadID'] = int(CONFIG['PartsForEachUploadID'])
        if CONFIG['ConcurrentUpParts'].lower() == 'true':
            CONFIG['ConcurrentUpParts'] = True
            if CONFIG['PartsForEachUploadID'] % CONFIG['ThreadsPerUser']:
                if CONFIG['PartsForEachUploadID'] < CONFIG['ThreadsPerUser']:
                    CONFIG['PartsForEachUploadID'] = CONFIG['ThreadsPerUser']
                else:
                    CONFIG['PartsForEachUploadID'] = int(
                        round(1.0 * CONFIG['PartsForEachUploadID'] / CONFIG['ThreadsPerUser']) * CONFIG[
                            'ThreadsPerUser'])
                logging.warning('change PartsForEachUploadID to %d' % CONFIG['PartsForEachUploadID'])
        else:
            CONFIG['ConcurrentUpParts'] = False

        CONFIG['PutTimesForOneObj'] = int(CONFIG['PutTimesForOneObj'])

        if CONFIG['MixLoopCount'] is not None and CONFIG['MixLoopCount']:
            CONFIG['MixLoopCount'] = int(CONFIG['MixLoopCount'])

        if CONFIG['RunSeconds']:
            CONFIG['RunSeconds'] = int(CONFIG['RunSeconds'])
        if CONFIG['TpsPerThread']:
            CONFIG['TpsPerThread'] = float(CONFIG['TpsPerThread'])
        if CONFIG['RecordDetails'].lower() == 'true':
            CONFIG['RecordDetails'] = True
        else:
            CONFIG['RecordDetails'] = False
        CONFIG['StatisticsInterval'] = int(CONFIG['StatisticsInterval'])
        if CONFIG['BadRequestCounted'].lower() == 'true':
            CONFIG['BadRequestCounted'] = True
        else:
            CONFIG['BadRequestCounted'] = False
        if CONFIG['AvoidSinBkOp'].lower() == 'true':
            CONFIG['AvoidSinBkOp'] = True
        else:
            CONFIG['AvoidSinBkOp'] = False
        if CONFIG['PrintProgress'].lower() == 'true':
            CONFIG['PrintProgress'] = True
        else:
            CONFIG['PrintProgress'] = False
        if CONFIG['LatencyPercentileMap'].lower() == 'true':
            CONFIG['LatencyPercentileMap'] = True
        else:
            CONFIG['LatencyPercentileMap'] = False
        if CONFIG['LatencyRequestsNumber'].lower() == 'true':
            CONFIG['LatencyRequestsNumber'] = True
        else:
            CONFIG['LatencyRequestsNumber'] = False
        if CONFIG['ObjNamePatternHash'].lower() == 'true':
            CONFIG['ObjNamePatternHash'] = True
        else:
            CONFIG['ObjNamePatternHash'] = False
        if CONFIG['CollectBasicData'].lower() == 'true':
            CONFIG['CollectBasicData'] = True
        else:
            CONFIG['CollectBasicData'] = False
        if CONFIG['IsMaster'].lower() == 'true':
            CONFIG['IsMaster'] = True
        else:
            CONFIG['IsMaster'] = False
        if CONFIG['IsRandomGet'].lower() == 'true':
            CONFIG['IsRandomGet'] = True
        else:
            CONFIG['IsRandomGet'] = False
        if CONFIG['IsRandomDelete'].lower() == 'true':
            CONFIG['IsRandomDelete'] = True
        else:
            CONFIG['IsRandomDelete'] = False
        if CONFIG['Anonymous'].lower() == 'true':
            CONFIG['Anonymous'] = True
        else:
            CONFIG['Anonymous'] = False
        if CONFIG['PutTimesForOnePart']:
            CONFIG['PutTimesForOnePart'] = int(CONFIG['PutTimesForOnePart'])

        CONFIG['StopWindowSeconds'] = int(CONFIG['StopWindowSeconds']) if CONFIG['StopWindowSeconds'] else 0
        CONFIG['RunWindowSeconds'] = int(CONFIG['RunWindowSeconds']) if CONFIG['RunWindowSeconds'] else 0
        if CONFIG['StopWindowSeconds'] > 0 and CONFIG['RunWindowSeconds'] > 0:
            CONFIG['WindowMode'] = True
            CONFIG['WindowTime'] = CONFIG['StopWindowSeconds'] + CONFIG['RunWindowSeconds']
        else:
            CONFIG['WindowMode'] = False
        if CONFIG['GetPositionFromMeta'].lower() == 'true':
            CONFIG['GetPositionFromMeta'] = True
        else:
            CONFIG['GetPositionFromMeta'] = False

        if CONFIG['IsDataFromFile'].lower() == 'true':
            CONFIG['IsDataFromFile'] = True
            if CONFIG['LocalFilePath'] is None or not CONFIG['LocalFilePath']:
                raise Exception('local file path is not provided.')
        else:
            CONFIG['IsDataFromFile'] = False
            CONFIG['LocalFilePath'] = None

        if CONFIG['IsCdn'].lower() == 'true':
            CONFIG['IsCdn'] = True
            if not CONFIG['CdnAK'] or not CONFIG['CdnSK'] or CONFIG['CdnSTSToken']:
                raise Exception('cdn ak or sk or stsToken is not provided.')
        else:
            CONFIG['IsCdn'] = False

        if CONFIG['IsHTTP2'].lower() == 'true':
            CONFIG['IsHTTP2'] = True
        else:
            CONFIG['IsHTTP2'] = False

        if CONFIG['TestNetwork'].lower() == 'true':
            CONFIG['TestNetwork'] = True
        else:
            CONFIG['TestNetwork'] = False
        if CONFIG['IsShareConnection'].lower() == 'true':
            CONFIG['IsShareConnection'] = True
        else:
            CONFIG['IsShareConnection'] = False

        if CONFIG['IsFileInterface'].lower() == 'true':
            CONFIG['IsFileInterface'] = True
        else:
            CONFIG['IsFileInterface'] = False

        if not ('processID' in CONFIG['ObjectNamePartten'] and 'ObjectNamePrefix' in CONFIG[
            'ObjectNamePartten'] and 'Index' in CONFIG['ObjectNamePartten']):
            raise Exception('both of processID,Index,ObjectNamePartten should be in config ObjectNamePartten')

        if CONFIG['IsDataFromFile'] and CONFIG['CalHashMD5']:
            raise Exception('IsDataFromFile and CalHashMD5 can not be true at the same time.')

        if CONFIG['IsHTTP2']:
            print '[Attention] currently, http2 is not stable in this test tool, make sure you have already set CalHashMD5 = false'

    except Exception, e:
        print '[ERROR] Read config file %s error: %s' % (config_file, e)
        sys.exit()


def initialize_object_index():
    global CONFIG, LIST_INDEX
    LIST_INDEX = range(int(CONFIG['ObjectsPerBucketPerThread']))


def is_needed_to_build_list_index():
    global CONFIG
    if str(CONFIG['Testcase']) == '202' and CONFIG['IsRandomGet']:
        return True
    elif str(CONFIG['Testcase']) == '204' and CONFIG['IsRandomDelete']:
        return True
    elif str(CONFIG['Testcase']) == '900':
        if '202' in CONFIG['MixOperations']:
            return True
        if '204' in CONFIG['MixOperations']:
            return True

    return False


def read_users():
    """
    load users.dat 
    """
    global USERS, CONFIG
    index = -1
    try:
        with open('./users.dat', 'r') as fd:
            for line in fd:
                if not line:
                    continue
                index += 1
                if index >= CONFIG['UserStartIndex'] and len(USERS) <= CONFIG['Users']:
                    user_info = line.strip()
                    user = User(user_info.split(',')[0], user_info.split(',')[1], user_info.split(',')[2])
                    USERS.append(user)
            fd.close()
        logging.debug("load user file end")
    except Exception, data:
        print "\033[1;31;40m[ERROR]\033[0m Load users Error, check file users.dat. Use iamPyTool.py to create users [%r]" % (
            data)
        logging.error(
            'Load users Error, check file users.dat. Use iamPyTool.py to create users')
        sys.exit()


def create_file_in_memory():
    f = StringIO()
    f.write(bytearray(random.getrandbits(8) for _ in xrange(1024 * 1024)))
    pos = random.randint(0, 4096)
    f.seek(pos, 0)

    return f


def generate_append_object_position():
    global CONFIG

    lines = []
    for i in range(int(CONFIG['ThreadsPerUser'])):
        bucket_name = CONFIG['BucketNameFixed']
        object_name = CONFIG['ObjectNamePrefix']
        obj_file = 'position/%s-%s-%s.dat' % (bucket_name, object_name, str(i))
        if os.path.exists(obj_file) and os.path.getsize(obj_file) > 0:
            logging.debug("read object from file: [%s] done." % obj_file)
            tmp = [tuple(map(str, line.rstrip('\n')[1:-1].split(','))) for line in open(obj_file, 'r')]
            lines.extend(tmp)
        else:
            logging.debug("file: [%s] does not exist. please check." % obj_file)
            return

    logging.debug("[%d] objects detected." % len(lines))

    return dict(lines)


def generate_image_process_parameters():
    global CONFIG

    if CONFIG['ImageManipulationType'] is not None and CONFIG['ImageManipulationType']:
        # CONFIG['x-image-process'] = 'image'
        params = ''
        if 'format' in CONFIG['ImageManipulationType'] and CONFIG['ImageFormat'] is not None and CONFIG['ImageFormat']:
            params = params + '/format,' + CONFIG['IamgeFormat']
        if 'resize' in CONFIG['ImageManipulationType'] and CONFIG['ResizeParams'] is not None and CONFIG['ResizeParams']:
            params = params + '/resize,' + CONFIG['ResizeParams']
        if 'crop' in CONFIG['ImageManipulationType'] and CONFIG['CropParams'] is not None and CONFIG['CropParams']:
            params = params + '/crop,' + CONFIG['CropParams']
        if 'info' in CONFIG['ImageManipulationType']:
            params = params + '/info'

        if params:
            CONFIG['x-image-process'] = 'image%s' % params
            logging.debug('image process param is: [%s]' % CONFIG['x-image-process'])
        else:
            raise Exception('ImageManipulationType or other parameters config is not correct.')
    else:
        raise Exception('ImageManipulationType or other parameters config is not correct.')


def list_user_buckets(process_id, user, conn, result_queue):
    request_type = 'ListUserBuckets'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    i = 0
    while i < CONFIG['RequestsPerThread']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求/限制TPS +　并发开始时间
            dst_time = i * 1.0 / CONFIG['TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        i += 1
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def create_bucket(process_id, user, conn, result_queue):
    request_type = 'CreateBucket'
    send_content = ''
    if CONFIG['BucketLocation']:
        send_content = '<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\
        <LocationConstraint>%s</LocationConstraint></CreateBucketConfiguration >' % random.choice(
            CONFIG['BucketLocation'].split(','))
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], send_content=send_content,
                                         virtual_host=CONFIG['VirtualHost'], domain_name=CONFIG['DomainName'],
                                         region=CONFIG['Region'], is_http2=CONFIG['IsHTTP2'], host=conn.host)
    if CONFIG['CreateWithACL']:
        rest.headers['x-amz-acl'] = CONFIG['CreateWithACL']
    if CONFIG['StorageClass']:
        if CONFIG['StorageClass'][-1:] == ',':
            CONFIG['StorageClass'] = CONFIG['StorageClass'][:-1]
        if CONFIG['StorageClass'].__contains__(','):
            storage_class_provided = CONFIG['StorageClass'].split(',')
            rest.headers['x-default-storage-class'] = random.choice(storage_class_provided)
        else:
            rest.headers['x-default-storage-class'] = CONFIG['StorageClass']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    if CONFIG['IsFileInterface']:
        rest.headers['x-obs-fs-file-interface'] = "Enabled"

    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    i = 0
    while i < CONFIG['BucketsPerUser']:
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        i += 1
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def list_objects_in_bucket(process_id, user, conn, result_queue):
    request_type = 'ListObjectsInBucket'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['max-keys'] = CONFIG['Max-keys']
    if CONFIG.__contains__('prefix') and CONFIG['prefix']:
        rest.queryArgs['prefix'] = CONFIG['prefix']
    i = 0
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    total_requests = 0
    while i < CONFIG['BucketsPerUser']:
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += 1
        marker = ''
        while marker is not None:
            if CONFIG['TpsPerThread']:  # 限制tps
                # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
                dst_time = total_requests * 1.0 / CONFIG['TpsPerThread'] + start_time
                wait_time = dst_time - time.time()
                if wait_time > 0:
                    time.sleep(wait_time)
            total_requests += 1
            rest.queryArgs['marker'] = urllib.unquote_plus(marker)
            resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
            marker = resp.return_data
            result_queue.put(
                (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
                 resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def head_bucket(process_id, user, conn, result_queue):
    request_type = 'HeadBucket'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def delete_bucket(process_id, user, conn, result_queue):
    request_type = 'DeleteBucket'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def bucket_delete(process_id, user, conn, result_queue):
    request_type = 'BucketDelete'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['deletebucket'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        rest.sendContent = '<?xml version="1.0" encoding="UTF-8"?><DeleteBucket><Bucket>' + rest.bucket + '</Bucket></DeleteBucket>'
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def options_bucket(process_id, user, conn, result_queue):
    request_type = 'OPTIONSBucket'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    if CONFIG['AllowedMethod']:
        if ',' in CONFIG['AllowedMethod']:
            rest.headers['Access-Control-Request-Method'] = []
            for i in CONFIG['AllowedMethod'].split(','):
                rest.headers['Access-Control-Request-Method'].append(i.upper())
        else:
            rest.headers['Access-Control-Request-Method'] = CONFIG['AllowedMethod'].upper()
    else:
        rest.headers['Access-Control-Request-Method'] = 'GET'
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    rest.headers['Origin'] = CONFIG['DomainName']
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def put_bucket_versioning(process_id, user, conn, result_queue):
    request_type = 'PutBucketVersioning'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk, auth_algorithm=CONFIG['AuthAlgorithm'],
                                         virtual_host=CONFIG['VirtualHost'], domain_name=CONFIG['DomainName'],
                                         region=CONFIG['Region'], is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['versioning'] = None
    rest.sendContent = '<VersioningConfiguration><Status>%s</Status></VersioningConfiguration>' % CONFIG[
        'VersionStatus']
    rest.headers['Content-MD5'] = base64.b64encode(hashlib.md5(rest.sendContent).digest())
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
            logging.info('bucket:' + rest.bucket)
        i += CONFIG['ThreadsPerUser']
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def get_bucket_versioning(process_id, user, conn, result_queue):
    request_type = 'GetBucketVersioning'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['versioning'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']

    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def put_bucket_website(process_id, user, conn, result_queue):
    request_type = 'PutBucketWebsite'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['website'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        rest.sendContent = '<WebsiteConfiguration><RedirectAllRequestsTo><HostName>' + CONFIG[
            'RedirectHostName'] + '</HostName></RedirectAllRequestsTo></WebsiteConfiguration>'
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def get_bucket_website(process_id, user, conn, result_queue):
    request_type = 'GetBucketWebsite'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['website'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def delete_bucket_website(process_id, user, conn, result_queue):
    request_type = 'DeleteBucketWebsite'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['website'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        rest.sendContent = '<WebsiteConfiguration><RedirectAllRequestsTo><HostName>' + CONFIG[
            'RedirectHostName'] + '</HostName></RedirectAllRequestsTo></WebsiteConfiguration>'
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def put_bucket_cors(process_id, user, conn, result_queue):
    request_type = 'PutBucketCORS'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['cors'] = None
    allow_method = ''
    if CONFIG['AllowedMethod']:
        if ',' in CONFIG['AllowedMethod']:
            for i in CONFIG['AllowedMethod'].split(','):
                allow_method += '<AllowedMethod>%s</AllowedMethod>' % i.upper()
        else:
            allow_method += '<AllowedMethod>%s</AllowedMethod>' % CONFIG['AllowedMethod'].upper()
    else:
        allow_method = '<AllowedMethod>GET</AllowedMethod>'

    rest.sendContent = '<CORSConfiguration><CORSRule>%s<AllowedOrigin>%s</AllowedOrigin></CORSRule></CORSConfiguration>' % \
                       (allow_method, CONFIG['DomainName'])
    rest.headers['Content-MD5'] = base64.b64encode(hashlib.md5(rest.sendContent).digest())
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def get_bucket_cors(process_id, user, conn, result_queue):
    request_type = 'GetBucketCORS'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['cors'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def delete_bucket_cors(process_id, user, conn, result_queue):
    request_type = 'DeleteBucketCORS'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['cors'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        rest.sendContent = '<WebsiteConfiguration><RedirectAllRequestsTo><HostName>' + CONFIG[
            'RedirectHostName'] + '</HostName></RedirectAllRequestsTo></WebsiteConfiguration>'
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def random_english(length):
    chars = string.ascii_letters + string.digits + '_-'
    return ''.join([random.choice(chars) for _ in range(length)])


def random_chinese(length):
    return ''.join([unichr(random.randint(0x4E00, 0x9FBF)).encode('utf-8') for _ in range(length)])


def put_bucket_tag(process_id, user, conn, result_queue):
    request_type = 'PutBucketTag'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['tagging'] = None
    key_values = 1
    if CONFIG['KeyValueNumber'] and int(CONFIG['KeyValueNumber']) <= 10:
        key_values = int(CONFIG['KeyValueNumber'])
    tempStr = ''
    for i in xrange(key_values):
        tempStr += '<Tag><Key>%s</Key><Value>%s</Value></Tag>' % (
            random.choice([random_chinese(random.randint(1, 36)), random_english(random.randint(1, 36))]),
            random.choice([random_chinese(random.randint(0, 43)), random_english(random.randint(0, 43))]))
    rest.sendContent = '<Tagging><TagSet>%s</TagSet></Tagging>' % tempStr
    rest.headers['Content-MD5'] = base64.b64encode(hashlib.md5(rest.sendContent).digest())
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def get_bucket_tag(process_id, user, conn, result_queue):
    request_type = 'GetBucketTag'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['tagging'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间

    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def delete_bucket_tag(process_id, user, conn, result_queue):
    request_type = 'DeleteBucketTag'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'])
    rest.queryArgs['tagging'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间

    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def get_bucket_multi_parts_upload(process_id, user, conn, result_queue):
    request_type = 'GetBucketMultiPartsUpload'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['uploads'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def get_bucket_location(process_id, user, conn, result_queue):
    request_type = 'GetBucketLocation'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['location'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def put_bucket_log(process_id, user, conn, result_queue):
    request_type = 'PutBucketLog'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['logging'] = None

    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
        target_bucket = CONFIG['BucketNameFixed']
    else:
        target_bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)

    rest.sendContent = '<?xml version="1.0" encoding="UTF-8"?><BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LoggingEnabled><TargetBucket>%s</TargetBucket><TargetPrefix>access_log</TargetPrefix></LoggingEnabled></BucketLoggingStatus>' % target_bucket
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def get_bucket_log(process_id, user, conn, result_queue):
    request_type = 'GetBucketLog'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['logging'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def get_bucket_storageinfo(process_id, user, conn, result_queue):
    request_type = 'GetBucketStorageInfo'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['storageinfo'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        rest.AuthAlgorithm = 'AWSV4'
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def put_bucket_storage_quota(process_id, user, conn, result_queue):
    request_type = 'PutBucketStorageQuota'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['quota'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    # if CONFIG['StorageQuota']:
    #     storagequota = int(CONFIG['StorageQuota'])
    # else:
    #     storagequota = 8
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        rest.AuthAlgorithm = 'AWSV4'
        rest.sendContent = '<Quota xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><StorageQuota>102400</StorageQuota></Quota>'
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


# add new function GetBucketStorageQuota
def get_bucket_storage_quota(process_id, user, conn, result_queue):
    request_type = 'GetBucketStorageQuota'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['quota'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        rest.AuthAlgorithm = 'AWSV4'
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


# add new function PutBucketAcl
def put_bucket_acl(process_id, user, conn, result_queue):
    request_type = 'PutBucketAcl'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    # rest.headers["x-amz-acl"] = 'private'
    rest.queryArgs['acl'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间

    user_id = 'domainiddomainiddomainiddo' + user.ak[len(user.ak) - 6:]
    displayname = 'domainnamedom' + user.ak[len(user.ak) - 6:]
    rest.sendContent = '''<?xml version="1.0" encoding="UTF-8"?>
<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner>
    <ID>%s</ID>
    <DisplayName>%s</DisplayName>
  </Owner>
  <AccessControlList>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
        <ID>%s</ID>
        <DisplayName>%s</DisplayName>
      </Grantee>
      <Permission>FULL_CONTROL</Permission>
    </Grant>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
        <URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
      </Grantee>
      <Permission>FULL_CONTROL</Permission>
    </Grant>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
        <URI>http://acs.amazonaws.com/groups/s3/LogDelivery</URI>
      </Grantee>
      <Permission>FULL_CONTROL</Permission>
    </Grant>
  </AccessControlList>
</AccessControlPolicy>''' % (user_id, displayname, user_id, displayname)
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


# add new function GetBucketAcl
def get_bucket_acl(process_id, user, conn, result_queue):
    request_type = 'GetBucketAcl'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['acl'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


# add new function PutBucketPolicy
def put_bucket_policy(process_id, user, conn, result_queue):
    request_type = 'PutBucketPolicy'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['policy'] = None
    i = process_id % CONFIG['ThreadsPerUser']

    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间

    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']

    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        # rest.AuthAlgorithm = 'AWSV4'
        rest.sendContent = '{"Version":"2008-10-17","Id":"aaaa-bbbb-cccc-dddd","Statement":[{"Sid":"1","Effect":"Allow","Principal":{"CanonicalUser":"*"},"Action":"s3:*","Resource":["arn:aws:s3:::%s"]}]}' % rest.bucket
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


# add new function GetBucketPolicy
def get_bucket_policy(process_id, user, conn, result_queue):
    request_type = 'GetBucketPolicy'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['policy'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        # rest.AuthAlgorithm = 'AWSV4'
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


# add new function DeleteBucketPolicy
def delete_bucket_policy(process_id, user, conn, result_queue):
    request_type = 'DeleteBucketPolicy'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['policy'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        # rest.AuthAlgorithm = 'AWSV4'
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


# add new function PutBucketLifecycle
def put_bucket_lifecycle(process_id, user, conn, result_queue):
    request_type = 'PutBucketLifecycle'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['lifecycle'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    rest.sendContent = '<LifecycleConfiguration><Rule><Prefix>%s</Prefix><Status>Enabled</Status><Expiration><Days>%d</Days></Expiration></Rule></LifecycleConfiguration>' % \
                       (CONFIG['BucketNameFixed'], 2)
    rest.headers['Content-MD5'] = base64.b64encode(hashlib.md5(rest.sendContent).digest())
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        # rest.AuthAlgorithm = 'AWSV4'
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


# add new function GetBucketLifecycle
def get_bucket_lifecycle(process_id, user, conn, result_queue):
    request_type = 'GetBucketLifecycle'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['lifecycle'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        # rest.AuthAlgorithm = 'AWSV4'
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


# add new function DeleteBucketLifecycle
def delete_bucket_lifecycle(process_id, user, conn, result_queue):
    request_type = 'DeleteBucketLifecycle'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['lifecycle'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        # rest.AuthAlgorithm = 'AWSV4'
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


# add new function PutBucketNotification
# 修改/opt/dfv/obs_service_layer/objectwebservice/osc/conf/obs_sod.properties  smn_connection = true
def put_bucket_notification(process_id, user, conn, result_queue):
    request_type = 'PutBucketNotification'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['notification'] = None
    rest.sendContent = '<NotificationConfiguration></NotificationConfiguration>'
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间

    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        # rest.AuthAlgorithm = 'AWSV4'
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


# add new function GetBucketNotification
def get_bucket_notification(process_id, user, conn, result_queue):
    request_type = 'GetBucketNotification'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['notification'] = None
    i = process_id % CONFIG['ThreadsPerUser']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = (i - process_id % CONFIG['ThreadsPerUser']) / CONFIG['ThreadsPerUser'] * 1.0 / CONFIG[
                'TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += CONFIG['ThreadsPerUser']
        # rest.AuthAlgorithm = 'AWSV4'
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def put_object(process_id, user, conn, result_queue):
    global SHARE_MEM
    request_type = 'PutObject'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_data_from_file=CONFIG['IsDataFromFile'],
                                         local_file_path=CONFIG['LocalFilePath'], is_http2=CONFIG['IsHTTP2'],
                                         host=conn.host)
    rest.headers['content-type'] = 'application/octet-stream'
    if CONFIG['ObjectStorageClass']:
        if CONFIG['ObjectStorageClass'][-1:] == ',':
            CONFIG['ObjectStorageClass'] = CONFIG['ObjectStorageClass'][:-1]
        if CONFIG['ObjectStorageClass'].__contains__(','):
            object_storage_class_provided = CONFIG['ObjectStorageClass'].split(',')
            rest.headers['x-amz-storage-class'] = random.choice(object_storage_class_provided)
        else:
            rest.headers['x-amz-storage-class'] = CONFIG['ObjectStorageClass']
    if CONFIG['PutWithACL']:
        rest.headers['x-amz-acl'] = CONFIG['PutWithACL']
    fixed_size = False
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    if CONFIG['ObjectNameFixed']:
        rest.key = CONFIG['ObjectNameFixed']
    if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
        rest.headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'
    elif CONFIG['SrvSideEncryptType'].lower() == 'sse-kms' and CONFIG['SrvSideEncryptAlgorithm'].lower() == 'aws:kms':
        rest.headers['x-amz-server-side-encryption'] = 'aws:kms'
        if CONFIG['SrvSideEncryptAWSKMSKeyId']:
            rest.headers['x-amz-server-side-encryption-aws-kms-key-id'] = CONFIG['SrvSideEncryptAWSKMSKeyId']
        if CONFIG['SrvSideEncryptContext']:
            rest.headers['x-amz-server-side-encryption-context'] = CONFIG['SrvSideEncryptContext']
    elif CONFIG['SrvSideEncryptType'].lower() == 'sse-kms' and CONFIG['SrvSideEncryptAlgorithm'].lower() == 'aes256':
        rest.headers['x-amz-server-side-encryption'] = 'AES256'
    # 如果打开CalHashMD5开关，在对象上传时写入一个自定义元数据，用于标记为本工具put上传的对象。
    if CONFIG['CalHashMD5']:
        rest.headers['x-amz-meta-md5written'] = 'yes'
    if CONFIG['Expires']:
        rest.headers['x-obs-expires'] = CONFIG['Expires']
    # 对象多版本，需要在上传后记录下版本号
    obj_v = ''
    obj_v_file = 'data/objv-%d.dat' % process_id
    open(obj_v_file, 'w').write(obj_v)
    # 错开每个并发起始选桶，避免单桶性能瓶颈。
    range_arr = range(0, CONFIG['BucketsPerUser'])
    if CONFIG['AvoidSinBkOp']:
        range_arr = range(process_id % CONFIG['BucketsPerUser'], CONFIG['BucketsPerUser']) + range(0, process_id % CONFIG['BucketsPerUser'])
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    buckets_cover = 0  # 已经遍历桶数量
    for i in range_arr:
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        j = 0
        while j < CONFIG['ObjectsPerBucketPerThread']:
            if not CONFIG['ObjectNameFixed']:
                if CONFIG['ObjectLexical']:
                    if not CONFIG['ObjNamePatternHash']:
                        rest.key = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                             str(
                                                                                                                 j)).replace(
                            'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                    else:
                        object_name = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                                str(
                                                                                                                    j)).replace(
                            'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                        rest.key = hashlib.md5(object_name).hexdigest() + '-' + object_name
                else:
                    rest.key = Util.random_string_create(random.randint(300, 900))
            if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
                rest.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(rest.key[-32:].zfill(32))
                rest.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                    hashlib.md5(rest.key[-32:].zfill(32)).digest())
                logging.debug('side-encryption-customer-key: [%r]' % rest.key[-32:].zfill(32))
            put_times_for_one_obj = CONFIG['PutTimesForOneObj']

            while put_times_for_one_obj > 0:
                if CONFIG['TpsPerThread']:  # 限制tps
                    # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求/限制TPS +　并发开始时间
                    dst_time = (buckets_cover * CONFIG['ObjectsPerBucketPerThread'] * CONFIG['PutTimesForOneObj'] + j *
                               CONFIG[
                                   'PutTimesForOneObj'] + (CONFIG['PutTimesForOneObj'] - put_times_for_one_obj)) * 1.0 / \
                              CONFIG['TpsPerThread'] + start_time
                    wait_time = dst_time - time.time()
                    if wait_time > 0:
                        time.sleep(wait_time)
                if CONFIG['WindowMode']:  # 运行窗口时间限制
                    window_time_now = (time.time() - valid_start_time.value) % CONFIG['WindowTime']
                    if window_time_now > CONFIG['RunWindowSeconds']:
                        time.sleep(CONFIG['WindowTime'] - window_time_now)
                if CONFIG['IsDataFromFile']:
                    rest.contentLength = int(os.path.getsize(CONFIG['LocalFilePath']))
                    fixed_size = True
                else:
                    if not fixed_size:
                        # change size every request for the same obj.
                        rest.contentLength, fixed_size = Util.generate_a_size(CONFIG['ObjectSize'])
                put_times_for_one_obj -= 1
                resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request(cal_md5=CONFIG['CalHashMD5'],
                                                                           memory_file=SHARE_MEM)
                result_queue.put(
                    (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
                     resp.end_time, resp.send_bytes, resp.recv_bytes, 'MD5:' + str(resp.content_md5),
                     resp.request_id, resp.status, resp.id2))
                if resp.return_data:
                    obj_v += '%s\t%s\t%s\n' % (rest.bucket, rest.key, resp.return_data)
                    # 每1KB，写入对象的versionID到本地文件objv-process_id.dat
                    if len(obj_v) >= 1024:
                        logging.info('write obj_v to file %s' % obj_v_file)
                        open(obj_v_file, 'a').write(obj_v)
                        obj_v = ''
            j += 1
        buckets_cover += 1
    if obj_v:
        open(obj_v_file, 'a').write(obj_v)


def append_object(process_id, user, conn, result_queue):
    global SHARE_MEM
    request_type = 'AppendObject'

    if CONFIG['GetPositionFromMeta']:
        logging.debug('Getting position from object meta')
        rest_head = obsPyCmd.OBSRequestDescriptor("HeadObject", ak=user.ak, sk=user.sk,
                                                  auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                                  domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                                  is_http2=CONFIG['IsHTTP2'], host=conn.host)

        rest_append = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                                    auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                                    domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                                    is_http2=CONFIG['IsHTTP2'], host=conn.host)

        # 处理head请求头域
        global OBJECTS
        if OBJECTS:
            handle_from_objects(request_type, rest_head, process_id, user, conn, result_queue)
            return

        elif not CONFIG['ObjectLexical']:
            logging.warn('Object name is not lexical, exit..')
            return

        if CONFIG['BucketNameFixed']:
            rest_head.bucket = CONFIG['BucketNameFixed']
        if CONFIG['ObjectNameFixed']:
            rest_head.key = CONFIG['ObjectNameFixed']
        if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
            rest_head.headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'
        start_time = None
        start_time_append = None
        if CONFIG['TpsPerThread']:
            start_time_head = time.time()  # 开始时间
            start_time_append = time.time()

        # 处理append object请求头域
        rest_append.headers['content-type'] = 'application/octet-stream'
        if CONFIG['ObjectStorageClass']:
            if CONFIG['ObjectStorageClass'][-1:] == ',':
                CONFIG['ObjectStorageClass'] = CONFIG['ObjectStorageClass'][:-1]
            if CONFIG['ObjectStorageClass'].__contains__(','):
                object_storage_class_provided = CONFIG['ObjectStorageClass'].split(',')
                rest_append.headers['x-amz-storage-class'] = random.choice(object_storage_class_provided)
            else:
                rest_append.headers['x-amz-storage-class'] = CONFIG['ObjectStorageClass']
        if CONFIG['PutWithACL']:
            rest_append.headers['x-amz-acl'] = CONFIG['PutWithACL']
        fixed_size = False
        if CONFIG['BucketNameFixed']:
            rest_append.bucket = CONFIG['BucketNameFixed']
        if CONFIG['ObjectNameFixed']:
            rest_append.key = CONFIG['ObjectNameFixed']
        if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
            rest_append.headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'
        elif CONFIG['SrvSideEncryptType'].lower() == 'sse-kms' and CONFIG['SrvSideEncryptAlgorithm'].lower() == 'aws:kms':
            rest_append.headers['x-amz-server-side-encryption'] = 'aws:kms'
            if CONFIG['SrvSideEncryptAWSKMSKeyId']:
                rest_append.headers['x-amz-server-side-encryption-aws-kms-key-id'] = CONFIG['SrvSideEncryptAWSKMSKeyId']
            if CONFIG['SrvSideEncryptContext']:
                rest_append.headers['x-amz-server-side-encryption-context'] = CONFIG['SrvSideEncryptContext']
        elif CONFIG['SrvSideEncryptType'].lower() == 'sse-kms' and CONFIG['SrvSideEncryptAlgorithm'].lower() == 'aes256':
            rest_append.headers['x-amz-server-side-encryption'] = 'AES256'
        # 如果打开CalHashMD5开关，在对象上传时写入一个自定义元数据，用于标记为本工具put上传的对象。
        if CONFIG['CalHashMD5']:
            rest_append.headers['x-amz-meta-md5written'] = 'yes'
        if CONFIG['Expires']:
            rest_append.headers['x-obs-expires'] = CONFIG['Expires']

        # 错开每个并发起始选桶，避免单桶性能瓶颈。
        rest_append.queryArgs["append"] = None

        range_arr = range(0, CONFIG['BucketsPerUser'])
        if CONFIG['AvoidSinBkOp']:
            range_arr = range(process_id % CONFIG['BucketsPerUser'], CONFIG['BucketsPerUser']) + range(0, process_id % CONFIG['BucketsPerUser'])

        buckets_cover = 0  # 已经遍历桶数量
        for i in range_arr:
            if not CONFIG['BucketNameFixed']:
                rest_head.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
                rest_append.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
            j = 0
            while j < CONFIG['ObjectsPerBucketPerThread']:
                if not CONFIG['ObjectNameFixed']:
                    if CONFIG['ObjectLexical']:
                        if not CONFIG['ObjNamePatternHash']:
                            key = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index', str(j)).replace('ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                        else:
                            object_name = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index', str(j)).replace('ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                            key = hashlib.md5(object_name).hexdigest() + '-' + object_name
                    else:
                        key = Util.random_string_create(random.randint(300, 900))
                    rest_head.key = key
                    rest_append.key = key
                if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
                    rest_head.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(rest_head.key[-32:].zfill(32))
                    rest_append.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(rest_append.key[-32:].zfill(32))
                    rest_head.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(hashlib.md5(rest_head.key[-32:].zfill(32)).digest())
                    rest_append.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(hashlib.md5(rest_append.key[-32:].zfill(32)).digest())
                    logging.debug('side-encryption-customer-key: [%r]' % rest_append.key[-32:].zfill(32))
                put_times_for_one_obj = CONFIG['PutTimesForOneObj']
                while put_times_for_one_obj > 0:
                    logging.debug("send Head object meta data request")
                    resp_head = obsPyCmd.OBSRequestHandler(rest_head, conn).make_request()
                    # 暂定不需要把head请求加入队列
                    # result_queue.put((process_id, user.username, rest_head.recordUrl, request_type, resp_head.start_time, resp_head.end_time, resp_head.send_bytes, resp_head.recv_bytes, '', resp_head.request_id, resp_head.status, resp_head.id2))
                    if CONFIG['IsHTTP2']:
                        rest_append.queryArgs["position"] = resp_head.position[0] if '200' in resp_head.status and resp_head.position else "0"
                    else:
                        rest_append.queryArgs["position"] = resp_head.position if resp_head.status == '200 OK' else "0"
                    logging.debug("position: [%s]" % str(resp_head.position))

                    if CONFIG['TpsPerThread']:  # 限制tps
                        # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求/限制TPS +　并发开始时间
                        dst_time_append = (buckets_cover * CONFIG['ObjectsPerBucketPerThread'] * CONFIG['PutTimesForOneObj'] + j * CONFIG['PutTimesForOneObj'] + (CONFIG['PutTimesForOneObj'] - put_times_for_one_obj)) * 1.0 / CONFIG['TpsPerThread'] + start_time_append
                        wait_time_append = dst_time_append - time.time()
                        if wait_time_append > 0:
                            time.sleep(wait_time_append)
                    if CONFIG['WindowMode']:  # 运行窗口时间限制
                        window_time_now = (time.time() - valid_start_time.value) % CONFIG['WindowTime']
                        if window_time_now > CONFIG['RunWindowSeconds']:
                            time.sleep(CONFIG['WindowTime'] - window_time_now)
                    if not fixed_size:
                        # change size every request for the same obj.
                        rest_append.contentLength, fixed_size = Util.generate_a_size(CONFIG['ObjectSize'])
                    put_times_for_one_obj -= 1

                    logging.debug("send append object request")
                    resp = obsPyCmd.OBSRequestHandler(rest_append, conn).make_request(cal_md5=CONFIG['CalHashMD5'],
                                                                                      memory_file=SHARE_MEM)
                    result_queue.put(
                        (process_id, user.username, rest_append.recordUrl, request_type, resp.start_time,
                         resp.end_time, resp.send_bytes, resp.recv_bytes, 'MD5:' + str(resp.content_md5),
                         resp.request_id, resp.status, resp.id2))
                j += 1
            buckets_cover += 1
    else:
        global APPEND_OBJECTS
        logging.debug("append object performance test")
        request_type = 'AppendObject'
        rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                             auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                             domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                             is_http2=CONFIG['IsHTTP2'], host=conn.host)
        rest.headers['content-type'] = 'application/octet-stream'
        if CONFIG['ObjectStorageClass']:
            if CONFIG['ObjectStorageClass'][-1:] == ',':
                CONFIG['ObjectStorageClass'] = CONFIG['ObjectStorageClass'][:-1]
            if CONFIG['ObjectStorageClass'].__contains__(','):
                object_storage_class_provided = CONFIG['ObjectStorageClass'].split(',')
                rest.headers['x-amz-storage-class'] = random.choice(object_storage_class_provided)
            else:
                rest.headers['x-amz-storage-class'] = CONFIG['ObjectStorageClass']
        if CONFIG['PutWithACL']:
            rest.headers['x-amz-acl'] = CONFIG['PutWithACL']
        fixed_size = False
        if CONFIG['BucketNameFixed']:
            rest.bucket = CONFIG['BucketNameFixed']
        if CONFIG['ObjectNameFixed']:
            rest.key = CONFIG['ObjectNameFixed']
        if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
            rest.headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'
        elif CONFIG['SrvSideEncryptType'].lower() == 'sse-kms' and CONFIG['SrvSideEncryptAlgorithm'].lower() == 'aws:kms':
            rest.headers['x-amz-server-side-encryption'] = 'aws:kms'
            if CONFIG['SrvSideEncryptAWSKMSKeyId']:
                rest.headers['x-amz-server-side-encryption-aws-kms-key-id'] = CONFIG['SrvSideEncryptAWSKMSKeyId']
            if CONFIG['SrvSideEncryptContext']:
                rest.headers['x-amz-server-side-encryption-context'] = CONFIG['SrvSideEncryptContext']
        elif CONFIG['SrvSideEncryptType'].lower() == 'sse-kms' and CONFIG['SrvSideEncryptAlgorithm'].lower() == 'aes256':
            rest.headers['x-amz-server-side-encryption'] = 'AES256'
        # 如果打开CalHashMD5开关，在对象上传时写入一个自定义元数据，用于标记为本工具put上传的对象。
        if CONFIG['CalHashMD5']:
            rest.headers['x-amz-meta-md5written'] = 'yes'
        if CONFIG['Expires']:
            rest.headers['x-obs-expires'] = CONFIG['Expires']

        # 如果position下有上传记录的对象名和历史写入的位置信息，从该文件读。
        obj_p_file = 'position/%s-%s-%d.dat' % (CONFIG['BucketNamePrefix'] if not CONFIG['BucketNameFixed'] else CONFIG['BucketNameFixed'],
                                                CONFIG['ObjectNamePrefix'] if not CONFIG['ObjectNameFixed'] else CONFIG['ObjectNameFixed'],
                                                process_id)

        # 判断该对象是否已经有position记录
        is_position_recorded = False
        if os.path.exists(obj_p_file) and os.path.getsize(obj_p_file) > 0 and len(APPEND_OBJECTS) > 0:
            is_position_recorded = True
            os.remove(obj_p_file)

        obj_p = ''
        obj_p_file = 'position/%s-%s-%d.dat' % (CONFIG['BucketNamePrefix'] if not CONFIG['BucketNameFixed'] else CONFIG['BucketNameFixed'],
                                                CONFIG['ObjectNamePrefix'] if not CONFIG['ObjectNameFixed'] else CONFIG['ObjectNameFixed'],
                                                process_id)
        open(obj_p_file, 'w').write(obj_p)

        rest.queryArgs["append"] = None

        # rest.queryArgs["position"] = "0"

        range_arr = range(0, CONFIG['BucketsPerUser'])
        if CONFIG['AvoidSinBkOp']:
            range_arr = range(process_id % CONFIG['BucketsPerUser'], CONFIG['BucketsPerUser']) + range(0, process_id % CONFIG['BucketsPerUser'])
        start_time = None
        if CONFIG['TpsPerThread']:
            start_time = time.time()  # 开始时间
        buckets_cover = 0  # 已经遍历桶数量
        for i in range_arr:
            if not CONFIG['BucketNameFixed']:
                rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
            j = 0
            while j < CONFIG['ObjectsPerBucketPerThread']:
                if not CONFIG['ObjectNameFixed']:
                    if CONFIG['ObjectLexical']:
                        if not CONFIG['ObjNamePatternHash']:
                            rest.key = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index', str(j)).replace('ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                        else:
                            object_name = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index', str(j)).replace('ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                            rest.key = hashlib.md5(object_name).hexdigest() + '-' + object_name
                    else:
                        rest.key = Util.random_string_create(random.randint(300, 900))
                if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
                    rest.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(
                        rest.key[-32:].zfill(32))
                    rest.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                        hashlib.md5(rest.key[-32:].zfill(32)).digest())
                    logging.debug('side-encryption-customer-key: [%r]' % rest.key[-32:].zfill(32))
                put_times_for_one_obj = CONFIG['PutTimesForOneObj']

                while put_times_for_one_obj > 0:
                    if CONFIG['TpsPerThread']:  # 限制tps
                        # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求/限制TPS +　并发开始时间
                        dst_time = (buckets_cover * CONFIG['ObjectsPerBucketPerThread'] * CONFIG['PutTimesForOneObj'] + j * CONFIG['PutTimesForOneObj'] + (CONFIG['PutTimesForOneObj'] - put_times_for_one_obj)) * 1.0 / CONFIG['TpsPerThread'] + start_time
                        wait_time = dst_time - time.time()
                        if wait_time > 0:
                            time.sleep(wait_time)
                    if CONFIG['WindowMode']:  # 运行窗口时间限制
                        window_time_now = (time.time() - valid_start_time.value) % CONFIG['WindowTime']
                        if window_time_now > CONFIG['RunWindowSeconds']:
                            time.sleep(CONFIG['WindowTime'] - window_time_now)
                    if not fixed_size:
                        # change size every request for the same obj.
                        rest.contentLength, fixed_size = Util.generate_a_size(CONFIG['ObjectSize'])
                    put_times_for_one_obj -= 1
                    if is_position_recorded:
                        rest.queryArgs["position"] = APPEND_OBJECTS[rest.key]
                    else:
                        rest.queryArgs["position"] = "0"
                    resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request(cal_md5=CONFIG['CalHashMD5'],
                                                                               memory_file=SHARE_MEM)
                    result_queue.put(
                        (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
                         resp.end_time, resp.send_bytes, resp.recv_bytes, 'MD5:' + str(resp.content_md5),
                         resp.request_id, resp.status, resp.id2))
                    # 更新对象追加写position
                    obj_p += '(%s,%s)\n' % (rest.key, resp.position)
                    # 每1KB，写入对象的position到本地文件objp-process_id.dat
                    if len(obj_p) >= 1024:
                        logging.info('write obj_v to file %s' % obj_p_file)
                        open(obj_p_file, 'a').write(obj_p)
                        obj_p = ''
                j += 1
            buckets_cover += 1
            if obj_p:
                open(obj_p_file, 'a').write(obj_p)


def image_process(process_id, user, conn, result_queue):
    request_type = 'ImageProcess'

    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_cdn=CONFIG['IsCdn'], cdn_ak=CONFIG['CdnAK'], cdn_sk=CONFIG['CdnSK'],
                                         cdn_sts_token=CONFIG['CdnSTSToken'])
    if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
        rest.headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'
    # 如果传入OBJECTS，则直接处理OBJECTS。
    global OBJECTS, LIST_INDEX
    if OBJECTS:
        handle_from_objects(request_type, rest, process_id, user, conn, result_queue)
        return

    # 如果data下有上传记录的对象名和版本，从该文件读。
    obj_v_file = 'data/objv-%d.dat' % process_id
    if os.path.exists(obj_v_file) and os.path.getsize(obj_v_file) > 0:
        handle_from_obj_v(request_type, obj_v_file, rest, process_id, user, conn, result_queue)
        return

    # 从字典序对象名下载。
    if not CONFIG['ObjectLexical']:
        logging.warn('Object name is not lexical, exit..')
        return

    i = 0
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    if CONFIG['ObjectNameFixed']:
        rest.key = CONFIG['ObjectNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        j = 0
        while j < CONFIG['ObjectsPerBucketPerThread']:
            if CONFIG['TpsPerThread']:  # 限制tps
                # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求/限制TPS +　并发开始时间
                dst_time = (i * CONFIG['ObjectsPerBucketPerThread'] + j) * 1.0 / CONFIG['TpsPerThread'] + start_time
                wait_time = dst_time - time.time()
                if wait_time > 0:
                    time.sleep(wait_time)
            if CONFIG['WindowMode']:  # 运行窗口时间限制
                window_time_now = (time.time() - valid_start_time.value) % CONFIG['WindowTime']
                if window_time_now > CONFIG['RunWindowSeconds']:
                    time.sleep(CONFIG['WindowTime'] - window_time_now)
            if not CONFIG['ObjectNameFixed']:
                if not CONFIG['ObjNamePatternHash']:
                    if not CONFIG['IsRandomGet']:
                        rest.key = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                             str(
                                                                                                                 j)).replace(
                            'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                    else:
                        rest.key = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                             str(
                                                                                                                 random.choice(
                                                                                                                     LIST_INDEX))).replace(
                            'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                else:
                    if not CONFIG['IsRandomGet']:
                        object_name = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                                str(
                                                                                                                    j)).replace(
                            'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                        rest.key = hashlib.md5(object_name).hexdigest() + '-' + object_name
                    else:
                        index = random.choice(LIST_INDEX)
                        object_name = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                                str(
                                                                                                                    index)).replace(
                            'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                        rest.key = hashlib.md5(object_name).hexdigest() + '-' + object_name
            if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
                rest.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(rest.key[-32:].zfill(32))
                rest.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                    hashlib.md5(rest.key[-32:].zfill(32)).digest())

            rest.queryArgs["x-image-process"] = CONFIG['x-image-process']

            resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request(cal_md5=CONFIG['CalHashMD5'])
            result_queue.put(
                (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
                 resp.end_time, resp.send_bytes, resp.recv_bytes, 'MD5:' + str(resp.content_md5),
                 resp.request_id, resp.status, resp.id2))
            j += 1
        i += 1


def handle_from_objects(request_type, rest, process_id, user, conn, result_queue):
    global OBJECTS
    objects_per_user = len(OBJECTS) / CONFIG['Threads']
    if objects_per_user == 0:
        if process_id >= len(OBJECTS):
            return
        else:
            start_index = end_index = process_id
    else:
        extra_obj = len(OBJECTS) % CONFIG['Threads']
        if process_id == 0:
            start_index = 0
            end_index = objects_per_user + extra_obj
        else:
            start_index = process_id * objects_per_user + extra_obj
            end_index = start_index + objects_per_user
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    pointer = start_index
    while pointer < end_index:
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求/限制TPS +　并发开始时间
            dst_time = (pointer - start_index) * 1.0 / CONFIG['TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if CONFIG['WindowMode']:  # 运行窗口时间限制
            window_time_now = (time.time() - valid_start_time.value) % CONFIG['WindowTime']
            if window_time_now > CONFIG['RunWindowSeconds']:
                time.sleep(CONFIG['WindowTime'] - window_time_now)
        rest.bucket = OBJECTS[pointer][:OBJECTS[pointer].find('/')]
        # 当Put对象时在obsPyCmd中，对对象名作了url的编译处理，此时如果要读取，则需要作反编译
        rest.key = urllib.unquote_plus(OBJECTS[pointer][OBJECTS[pointer].find('/') + 1:])
        if CONFIG['Testcase'] in (202,) and CONFIG['Range']:
            rest.headers['Range'] = 'bytes=%s' % random.choice(CONFIG['Range'].split(';')).strip()
        pointer += 1
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request(cal_md5=CONFIG['CalHashMD5'])
        if CONFIG["Testcase"] in (202,):
            result_queue.put(
                (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
                 resp.end_time, resp.send_bytes, resp.recv_bytes, 'MD5:' + str(resp.content_md5),
                 resp.request_id, resp.status, resp.id2))
        else:
            result_queue.put(
                (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
                 resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def handle_from_obj_v(request_type, obj_v_file, rest, process_id, user, conn, result_queue):
    logging.debug("generate object name from obj_v")
    obj_v_file_read = open(obj_v_file, 'r')
    obj = obj_v_file_read.readline()
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    total_requests = 0
    while obj:
        if obj and len(obj.split('\t')) != 3:
            logging.warning('obj [%r] format error in file %s' % (obj, obj_v_file))
            continue
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求/限制TPS +　并发开始时间
            dst_time = total_requests * 1.0 / CONFIG['TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        if CONFIG['WindowMode']:  # 运行窗口时间限制
            window_time_now = (time.time() - valid_start_time.value) % CONFIG['WindowTime']
            if window_time_now > CONFIG['RunWindowSeconds']:
                time.sleep(CONFIG['WindowTime'] - window_time_now)
        total_requests += 1
        obj = obj[:-1]
        rest.bucket = obj.split('\t')[0]
        rest.key = obj.split('\t')[1]
        rest.queryArgs['versionId'] = obj.split('\t')[2]
        obj = obj_v_file_read.readline()
        if rest.requestType == 'GetObject':
            if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
                rest.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(rest.key[-32:].zfill(32))
                rest.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                    hashlib.md5(rest.key[-32:].zfill(32)).digest())
            resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request(cal_md5=CONFIG['CalHashMD5'])
        else:
            resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, 'MD5:' + str(resp.content_md5),
             resp.request_id, resp.status, resp.id2))


def get_object(process_id, user, conn, result_queue):
    request_type = 'GetObject'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_cdn=CONFIG['IsCdn'], cdn_ak=CONFIG['CdnAK'], cdn_sk=CONFIG['CdnSK'],
                                         cdn_sts_token=CONFIG['CdnSTSToken'], is_http2=CONFIG['IsHTTP2'], host=conn.host)
    if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
        rest.headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'
    if CONFIG['Testcase'] in (202, 900) and CONFIG['Range']:
        rest.headers['Range'] = 'bytes=%s' % random.choice(CONFIG['Range'].split(';')).strip()
    # 如果传入OBJECTS，则直接处理OBJECTS。
    global OBJECTS, LIST_INDEX
    if OBJECTS:
        handle_from_objects(request_type, rest, process_id, user, conn, result_queue)
        return

    # 如果data下有上传记录的对象名和版本，从该文件读。
    obj_v_file = 'data/objv-%d.dat' % process_id
    if os.path.exists(obj_v_file) and os.path.getsize(obj_v_file) > 0:
        handle_from_obj_v(request_type, obj_v_file, rest, process_id, user, conn, result_queue)
        return

    # 从字典序对象名下载。
    if not CONFIG['ObjectLexical']:
        logging.warn('Object name is not lexical, exit..')
        return

    i = 0
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    if CONFIG['ObjectNameFixed']:
        rest.key = CONFIG['ObjectNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        j = 0
        while j < CONFIG['ObjectsPerBucketPerThread']:
            if CONFIG['TpsPerThread']:  # 限制tps
                # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求/限制TPS +　并发开始时间
                dst_time = (i * CONFIG['ObjectsPerBucketPerThread'] + j) * 1.0 / CONFIG['TpsPerThread'] + start_time
                wait_time = dst_time - time.time()
                if wait_time > 0:
                    time.sleep(wait_time)
            if CONFIG['WindowMode']:  # 运行窗口时间限制
                window_time_now = (time.time() - valid_start_time.value) % CONFIG['WindowTime']
                if window_time_now > CONFIG['RunWindowSeconds']:
                    time.sleep(CONFIG['WindowTime'] - window_time_now)
            if CONFIG['Range']:
                rest.headers['Range'] = 'bytes=%s' % random.choice(CONFIG['Range'].split(';')).strip()
            if not CONFIG['ObjectNameFixed']:
                if not CONFIG['ObjNamePatternHash']:
                    if not CONFIG['IsRandomGet']:
                        rest.key = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                             str(
                                                                                                                 j)).replace(
                            'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                    else:
                        rest.key = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                             str(
                                                                                                                 random.choice(
                                                                                                                     LIST_INDEX))).replace(
                            'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                else:
                    if not CONFIG['IsRandomGet']:
                        object_name = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                                str(
                                                                                                                    j)).replace(
                            'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                        rest.key = hashlib.md5(object_name).hexdigest() + '-' + object_name
                    else:
                        index = random.choice(LIST_INDEX)
                        object_name = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                                str(
                                                                                                                    index)).replace(
                            'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                        rest.key = hashlib.md5(object_name).hexdigest() + '-' + object_name
            if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
                rest.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(rest.key[-32:].zfill(32))
                rest.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                    hashlib.md5(rest.key[-32:].zfill(32)).digest())

            resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request(cal_md5=CONFIG['CalHashMD5'])
            result_queue.put(
                (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
                 resp.end_time, resp.send_bytes, resp.recv_bytes, 'MD5:' + str(resp.content_md5),
                 resp.request_id, resp.status, resp.id2))
            j += 1
        i += 1


def head_object(process_id, user, conn, result_queue):
    request_type = 'HeadObject'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    # 如果传入OBJECTS，则直接处理OBJECTS。
    global OBJECTS
    if OBJECTS:
        handle_from_objects(request_type, rest, process_id, user, conn, result_queue)
        return

    elif not CONFIG['ObjectLexical']:
        logging.warn('Object name is not lexical, exit..')
        return
    i = 0
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    if CONFIG['ObjectNameFixed']:
        rest.key = CONFIG['ObjectNameFixed']
    if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
        rest.headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        j = 0
        while j < CONFIG['ObjectsPerBucketPerThread']:
            if CONFIG['TpsPerThread']:  # 限制tps
                # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求/限制TPS +　并发开始时间
                dst_time = (i * CONFIG['ObjectsPerBucketPerThread'] + j) * 1.0 / CONFIG['TpsPerThread'] + start_time
                wait_time = dst_time - time.time()
                if wait_time > 0:
                    time.sleep(wait_time)
            if not CONFIG['ObjectNameFixed']:
                if not CONFIG['ObjNamePatternHash']:
                    rest.key = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index', str(
                        j)).replace('ObjectNamePrefix',
                                    CONFIG[
                                        'ObjectNamePrefix'])
                else:
                    object_name = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                            str(
                                                                                                                j)).replace(
                        'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                    rest.key = hashlib.md5(object_name).hexdigest() + '-' + object_name
            if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
                rest.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(rest.key[-32:].zfill(32))
                rest.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                    hashlib.md5(rest.key[-32:].zfill(32)).digest())

            resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
            result_queue.put(
                (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
                 resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))
            j += 1
        i += 1


def delete_object(process_id, user, conn, result_queue):
    request_type = 'DeleteObject'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)

    # 如果传入OBJECTS，则直接处理OBJECTS。
    global OBJECTS, LIST_INDEX
    if OBJECTS:
        handle_from_objects(request_type, rest, process_id, user, conn, result_queue)
        return

    # 如果data下有上传记录的对象名和版本，从该文件读。
    obj_v_file = 'data/objv-%d.dat' % process_id
    if os.path.exists(obj_v_file) and os.path.getsize(obj_v_file) > 0:
        handle_from_obj_v(request_type, obj_v_file, rest, process_id, user, conn, result_queue)
        return

    elif not CONFIG['ObjectLexical']:
        logging.warn('Object name is not lexical, exit..')
        return

    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    if CONFIG['ObjectNameFixed']:
        rest.key = CONFIG['ObjectNameFixed']
    range_arr = range(0, CONFIG['BucketsPerUser'])
    # 错开每个并发起始选桶，避免单桶性能瓶颈。
    if CONFIG['AvoidSinBkOp']:
        range_arr = range(process_id % CONFIG['BucketsPerUser'], CONFIG['BucketsPerUser']) + range(0,
                                                                                                   process_id % CONFIG[
                                                                                                       'BucketsPerUser'])
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    buckets_cover = 0  # 已经遍历桶数量
    for i in range_arr:
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += 1
        j = 0
        while j < CONFIG['ObjectsPerBucketPerThread']:
            if CONFIG['TpsPerThread']:  # 限制tps
                # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求/限制TPS +　并发开始时间
                dst_time = (buckets_cover * CONFIG['ObjectsPerBucketPerThread'] + j) * 1.0 / CONFIG[
                    'TpsPerThread'] + start_time
                wait_time = dst_time - time.time()
                if wait_time > 0:
                    time.sleep(wait_time)
            if CONFIG['WindowMode']:  # 运行窗口时间限制
                window_time_now = (time.time() - valid_start_time.value) % CONFIG['WindowTime']
                if window_time_now > CONFIG['RunWindowSeconds']:
                    time.sleep(CONFIG['WindowTime'] - window_time_now)
            if not CONFIG['ObjectNameFixed']:
                if not CONFIG['ObjNamePatternHash']:
                    if not CONFIG['IsRandomDelete']:
                        rest.key = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                             str(
                                                                                                                 j)).replace(
                            'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                    else:
                        index = random.choice(LIST_INDEX)
                        LIST_INDEX.remove(index)
                        rest.key = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                             str(
                                                                                                                 index)).replace(
                            'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                else:
                    if not CONFIG['IsRandomDelete']:
                        object_name = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                                str(
                                                                                                                    j)).replace(
                            'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                        rest.key = hashlib.md5(object_name).hexdigest() + '-' + object_name
                    else:
                        index = random.choice(LIST_INDEX)
                        LIST_INDEX.remove(index)
                        object_name = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                                str(
                                                                                                                    index)).replace(
                            'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                        rest.key = hashlib.md5(object_name).hexdigest() + '-' + object_name
            resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
            result_queue.put(
                (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
                 resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))
            j += 1
        buckets_cover += 1


def restore_object(process_id, user, conn, result_queue):
    request_type = 'RestoreObject'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)

    # 如果传入OBJECTS，则直接处理OBJECTS。
    global OBJECTS
    if OBJECTS:
        handle_from_objects(request_type, rest, process_id, user, conn, result_queue)
        return

    rest.queryArgs['restore'] = None
    rest.sendContent = '<RestoreRequest xmlns="http://s3.amazonaws.com/doc/2006-3-01"><Days>%s</Days><GlacierJobParameters><Tier>%s</Tier></GlacierJobParameters></RestoreRequest>' % (
    CONFIG['RestoreDays'], CONFIG['RestoreTier'])
    rest.headers['Content-MD5'] = base64.b64encode(hashlib.md5(rest.sendContent).digest())
    # 如果data下有上传记录的对象名和版本，从该文件读。
    obj_v_file = 'data/objv-%d.dat' % process_id
    if os.path.exists(obj_v_file) and os.path.getsize(obj_v_file) > 0:
        handle_from_obj_v(request_type, obj_v_file, rest, process_id, user, conn, result_queue)
        return

    elif not CONFIG['ObjectLexical']:
        logging.warn('Object name is not lexical, exit..')
        return

    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    if CONFIG['ObjectNameFixed']:
        rest.key = CONFIG['ObjectNameFixed']
    rest.queryArgs['restore'] = None
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    i = 0
    while i < CONFIG['BucketsPerUser']:
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        j = 0
        while j < CONFIG['ObjectsPerBucketPerThread']:
            if CONFIG['TpsPerThread']:  # 限制tps
                # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求/限制TPS +　并发开始时间
                dst_time = (i * CONFIG['ObjectsPerBucketPerThread'] + j) * 1.0 / CONFIG['TpsPerThread'] + start_time
                wait_time = dst_time - time.time()
                if wait_time > 0:
                    time.sleep(wait_time)
            if not CONFIG['ObjectNameFixed']:
                if not CONFIG['ObjNamePatternHash']:
                    rest.key = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index', str(
                        j)).replace('ObjectNamePrefix',
                                    CONFIG[
                                        'ObjectNamePrefix'])
                else:
                    object_name = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                            str(
                                                                                                                j)).replace(
                        'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                    rest.key = hashlib.md5(object_name).hexdigest() + '-' + object_name
            rest.sendContent = '<RestoreRequest xmlns="http://s3.amazonaws.com/doc/2006-3-01"><Days>%s</Days><GlacierJobParameters><Tier>%s</Tier></GlacierJobParameters></RestoreRequest>' % (
                CONFIG['RestoreDays'], CONFIG['RestoreTier'])
            logging.debug('send content [%s] ' % rest.sendContent)
            rest.headers['Content-MD5'] = base64.b64encode(hashlib.md5(rest.sendContent).digest())
            resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
            result_queue.put(
                (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
                 resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))
            j += 1
        i += 1


def delete_multi_objects(process_id, user, conn, result_queue):
    if not CONFIG['ObjectLexical']:
        logging.warn('Object name is not lexical, exit..')
        return
    if CONFIG['ObjectsPerBucketPerThread'] <= 0:
        logging.warn('ObjectsPerBucketPerThread <= 0, exit..')
        return
    request_type = 'DeleteMultiObjects'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['delete'] = None
    i = 0
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        i += 1
        delete_times_per_bucket = math.ceil(
            CONFIG['ObjectsPerBucketPerThread'] * 1.0 / CONFIG['DeleteObjectsPerRequest'])
        logging.debug('ObjectsPerBucketPerThread: %d, DeleteObjectsPerRequest: %d, delete_times_per_bucket:%d' % (
            CONFIG['ObjectsPerBucketPerThread'], CONFIG['DeleteObjectsPerRequest'], delete_times_per_bucket))
        j = 0
        while j < CONFIG['ObjectsPerBucketPerThread']:
            if CONFIG['TpsPerThread']:  # 限制tps
                # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求/限制TPS +　并发开始时间
                dst_time = (i * math.ceil(CONFIG['ObjectsPerBucketPerThread'] / CONFIG['DeleteObjectsPerRequest']) + j /
                           CONFIG[
                               'DeleteObjectsPerRequest']) * 1.0 / CONFIG['TpsPerThread'] + start_time
                wait_time = dst_time - time.time()
                if wait_time > 0:
                    time.sleep(wait_time)
            rest.sendContent = '<Delete>'
            k = 0
            while k < CONFIG['DeleteObjectsPerRequest']:
                if j >= CONFIG['ObjectsPerBucketPerThread']:
                    break
                if not CONFIG['ObjNamePatternHash']:
                    key = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                    str(j)).replace(
                        'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                else:
                    object_name = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                            str(
                                                                                                                j)).replace(
                        'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                    key = hashlib.md5(object_name).hexdigest() + '-' + object_name

                rest.sendContent += '<Object><Key>%s</Key></Object>' % key
                k += 1
                j += 1
            rest.sendContent += '</Delete>'
            logging.debug('send content [%s] ' % rest.sendContent)
            rest.headers['Content-MD5'] = base64.b64encode(hashlib.md5(rest.sendContent).digest())
            resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
            result_queue.put(
                (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
                 resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def copy_object(process_id, user, conn, result_queue):
    if not CONFIG['ObjectLexical']:
        logging.warn('Object name is not lexical, exit..')
        return
    request_type = 'CopyObject'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.headers['x-amz-acl'] = 'public-read-write'
    rest.headers['x-amz-metadata-directive'] = 'COPY'
    if CONFIG['copySrcObjFixed']:
        rest.headers['x-amz-copy-source'] = '/' + CONFIG['copySrcObjFixed']
    if CONFIG['copyDstObjFixed']:
        rest.bucket = CONFIG['copyDstObjFixed'].split('/')[0]
        rest.key = CONFIG['copyDstObjFixed'].split('/')[1]
    elif CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
        rest.headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'
    if CONFIG['copySrcSrvSideEncryptType'].lower() == 'sse-c':
        rest.headers['x-amz-copy-source-server-side-encryption-customer-algorithm'] = 'AES256'
    if CONFIG['SrvSideEncryptType'].lower() == 'sse-kms' and CONFIG['SrvSideEncryptAlgorithm'].lower() == 'aws:kms':
        rest.headers['x-amz-server-side-encryption'] = 'aws:kms'
        if CONFIG['SrvSideEncryptAWSKMSKeyId']:
            rest.headers['x-amz-server-side-encryption-aws-kms-key-id'] = CONFIG['SrvSideEncryptAWSKMSKeyId']
        if CONFIG['SrvSideEncryptContext']:
            rest.headers['x-amz-server-side-encryption-context'] = CONFIG['SrvSideEncryptContext']
    elif CONFIG['SrvSideEncryptType'].lower() == 'sse-kms' and CONFIG['SrvSideEncryptAlgorithm'].lower() == 'aes256':
        rest.headers['x-amz-server-side-encryption'] = 'AES256'
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    i = 0
    while i < CONFIG['BucketsPerUser']:
        # 如果未配置目的对象和固定桶，设置目的桶为源对象所在的桶
        if not CONFIG['copyDstObjFixed'] and not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        j = 0
        while j < CONFIG['ObjectsPerBucketPerThread']:
            if CONFIG['TpsPerThread']:  # 限制tps
                # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求/限制TPS +　并发开始时间
                dst_time = (i * CONFIG['ObjectsPerBucketPerThread'] + j) * 1.0 / CONFIG['TpsPerThread'] + start_time
                wait_time = dst_time - time.time()
                if wait_time > 0:
                    time.sleep(wait_time)
            if not CONFIG['copyDstObjFixed']:
                if not CONFIG['ObjNamePatternHash']:
                    rest.key = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index', str(j)).replace('ObjectNamePrefix', CONFIG['ObjectNamePrefix'] + '.copy')
                else:
                    object_name = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index', str(j)).replace('ObjectNamePrefix', CONFIG['ObjectNamePrefix'] + '.copy')
                    rest.key = hashlib.md5(object_name).hexdigest() + '-' + object_name
            if not CONFIG['copySrcObjFixed']:
                if not CONFIG['ObjNamePatternHash']:
                    rest.headers['x-amz-copy-source'] = '/%s/%s' % (rest.bucket, CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index', str(j)).replace('ObjectNamePrefix', CONFIG['ObjectNamePrefix']))
                else:
                    object_name = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index', str(j)).replace('ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                    key = hashlib.md5(object_name).hexdigest() + '-' + object_name
                    rest.headers['x-amz-copy-source'] = '/%s/%s' % (rest.bucket, key)
            if CONFIG['copySrcSrvSideEncryptType'].lower() == 'sse-c':
                src_en_key = rest.headers['x-amz-copy-source'].split('/')[2][-32:].zfill(32)
                rest.headers['x-amz-copy-source-server-side-encryption-customer-key'] = base64.b64encode(src_en_key)
                rest.headers['x-amz-copy-source-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                    hashlib.md5(src_en_key).digest())
                logging.debug('src encrpt key: %s, src encrypt key md5: %s' % (
                    src_en_key, rest.headers['x-amz-copy-source-server-side-encryption-customer-key-MD5']))
            if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
                rest.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(rest.key[-32:].zfill(32))
                rest.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                    hashlib.md5(rest.key[-32:].zfill(32)).digest())
            resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
            # 同拷贝对象，若拷贝段操作先返回200 OK，并不代表拷贝成功。如果返回了200，但没有获取到ETag，将response修改为500错误。
            if resp.status.startswith('200 ') and not resp.return_data:
                logging.warning('response 200 OK without ETag, set status code 500 InternalError')
                resp.status = '500 InternalError'
            result_queue.put(
                (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
                 resp.end_time, resp.send_bytes, resp.recv_bytes,
                 'copySrc:' + rest.headers['x-amz-copy-source'], resp.request_id, resp.status, resp.id2))
            j += 1
        i += 1


def init_multi_upload(process_id, user, conn, result_queue):
    # if not CONFIG['ObjectLexical']:
    #     logging.warn('Object name is not lexical, exit..')
    #     return
    if CONFIG['ObjectsPerBucketPerThread'] <= 0 or CONFIG['BucketsPerUser'] <= 0:
        logging.warn('ObjectsPerBucketPerThread or BucketsPerUser <= 0, exit..')
        return
    request_type = 'InitMultiUpload'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.queryArgs['uploads'] = None
    if CONFIG['MultiUploadStorageClass']:
        if CONFIG['MultiUploadStorageClass'][-1:] == ',':
            CONFIG['MultiUploadStorageClass'] = CONFIG['MultiUploadStorageClass'][:-1]
        if CONFIG['MultiUploadStorageClass'].__contains__(','):
            multi_upload_storage_class_provided = CONFIG['MultiUploadStorageClass'].split(',')
            rest.headers['x-amz-storage-class'] = random.choice(multi_upload_storage_class_provided)
        else:
            rest.headers['x-amz-storage-class'] = CONFIG['MultiUploadStorageClass']
    if CONFIG['PutWithACL']:
        rest.headers['x-amz-acl'] = CONFIG['PutWithACL']
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    if CONFIG['ObjectNameFixed']:
        rest.key = CONFIG['ObjectNameFixed']
    if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
        rest.headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'
    elif CONFIG['SrvSideEncryptType'].lower() == 'sse-kms' and CONFIG['SrvSideEncryptAlgorithm'].lower() == 'aws:kms':
        rest.headers['x-amz-server-side-encryption'] = 'aws:kms'
        if CONFIG['SrvSideEncryptAWSKMSKeyId']:
            rest.headers['x-amz-server-side-encryption-aws-kms-key-id'] = CONFIG['SrvSideEncryptAWSKMSKeyId']
        if CONFIG['SrvSideEncryptContext']:
            rest.headers['x-amz-server-side-encryption-context'] = CONFIG['SrvSideEncryptContext']
    elif CONFIG['SrvSideEncryptType'].lower() == 'sse-kms' and CONFIG['SrvSideEncryptAlgorithm'].lower() == 'aes256':
        rest.headers['x-amz-server-side-encryption'] = 'AES256'
    if CONFIG['Expires']:
        rest.headers['x-obs-expires'] = CONFIG['Expires']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    upload_ids = ''
    i = 0
    while i < CONFIG['BucketsPerUser']:
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        j = 0
        while j < CONFIG['ObjectsPerBucketPerThread']:
            if CONFIG['TpsPerThread']:  # 限制tps
                # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求/限制TPS +　并发开始时间
                dst_time = (i * CONFIG['ObjectsPerBucketPerThread'] + j) * 1.0 / CONFIG['TpsPerThread'] + start_time
                wait_time = dst_time - time.time()
                if wait_time > 0:
                    time.sleep(wait_time)
            if not CONFIG['ObjectNameFixed']:
                if CONFIG['ObjectLexical']:
                    if not CONFIG['ObjNamePatternHash']:
                        rest.key = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                             str(
                                                                                                                 j)).replace(
                            'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                    else:
                        object_name = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                                str(
                                                                                                                    j)).replace(
                            'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                        rest.key = hashlib.md5(object_name).hexdigest() + '-' + object_name
                else:
                    rest.key = Util.random_string_create(random.randint(300, 900))
            if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
                rest.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(rest.key[-32:].zfill(32))
                rest.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                    hashlib.md5(rest.key[-32:].zfill(32)).digest())
            resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
            result_queue.put(
                (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
                 resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))
            # 如果请求成功，记录return_data（UploadId)到本地文件
            if resp.status.startswith('200 '):
                logging.debug('rest.key:%s, rest.returndata:%s' % (rest.key, resp.return_data))
                upload_ids += '%s\t%s\t%s\t%s\n' % (user.username, rest.bucket, rest.key, resp.return_data)
            j += 1
        i += 1
    if upload_ids == '':
        return None
    # 退出前，写统计结果到本地文件
    uploadid_writer = None
    uploadid_file = 'data/upload_id-%d.dat' % process_id
    try:
        uploadid_writer = open(uploadid_file, 'w')
        uploadid_writer.write(upload_ids)
    except Exception, data:
        logging.error('process [%d] write upload_ids error %s' % (process_id, data))
    finally:
        if uploadid_writer:
            try:
                uploadid_writer.close()
            except IOError:
                pass


def upload_part(process_id, user, conn, result_queue):
    # 从本地加载本进程需要做的upload_ids。考虑到单upload_id多并发上传段场景，需要加载其它进程初始化的upload_ids。
    # 如5个用户，每用户2个并发，则每个upload_id可以最大2个并发上传段。
    # upload_id-0(usr0,p0)  upload_id-1(usr0,p1)  upload_id-2(usr1,p2)  upload_id-3(usr1,p3) upload_id-4(usr2,p4)
    # upload_id-5(usr2,p5)  upload_id-6(usr3,p6)  upload_id-7(usr3,p7)  upload_id-8(usr4,p8) upload_id-9(usr4,p9)
    # p0,p1需要顺序加载usr0,p0和usr0,p1
    upload_ids = []
    if not CONFIG['ConcurrentUpParts']:
        id_files = [process_id]
    else:
        id_files = range(process_id / CONFIG['ThreadsPerUser'] * CONFIG['ThreadsPerUser'],
                         (process_id / CONFIG['ThreadsPerUser'] + 1) * CONFIG['ThreadsPerUser'])
    for i in id_files:
        upload_id_file = 'data/upload_id-%d.dat' % i
        try:
            with open(upload_id_file, 'r') as fd:
                for line in fd:
                    if line.strip() == '':
                        continue
                    # 如果非本并发的用户初始化的upload_id，跳过。
                    if not line.startswith(user.username + '\t'):
                        continue
                    if len(line.split('\t')) != 4:
                        logging.warn('upload_ids record error [%s]' % line)
                        continue
                    # 记录upload_id的原并发号i
                    upload_ids.append((str(i) + '.' + line.strip()).split('\t'))
                fd.close()
            logging.info('process %d load upload_ids file %s end' % (process_id, upload_id_file))
        except Exception, data:
            logging.error("load %s for process %d error, [%r], exit" % (upload_id_file, process_id, data))
            continue

    if not upload_ids:
        logging.warning("load no upload_ids for process %d, from file upload_id-%r exit" % (process_id, id_files))
        return
    else:
        logging.info("total load %d upload_ids" % len(upload_ids))

    fixed_size = False
    request_type = 'UploadPart'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.headers['content-type'] = 'application/octet-stream'
    if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
        rest.headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    total_requests = 0
    for upload_id in upload_ids:
        rest.bucket = upload_id[1]
        rest.key = upload_id[2]
        rest.queryArgs['uploadId'] = upload_id[3]
        parts_record = ''
        # 如果开启了并发上传段，本并发只处理部分段。
        if not CONFIG['ConcurrentUpParts']:
            part_ids = range(1, CONFIG['PartsForEachUploadID'] + 1)
        else:
            part_ids = range(process_id % CONFIG['ThreadsPerUser'] + 1, CONFIG['PartsForEachUploadID'] + 1,
                             CONFIG['ThreadsPerUser'])
        logging.debug('process %d handle parts: %r' % (process_id, part_ids))
        if not part_ids:
            logging.warning(
                'process %d has no parts to do for upload_id %s, break' % (process_id, rest.queryArgs['uploadId']))
            continue
        for i in part_ids:
            if CONFIG['TpsPerThread']:  # 限制tps
                # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
                dst_time = total_requests * 1.0 / CONFIG['TpsPerThread'] + start_time
                wait_time = dst_time - time.time()
                if wait_time > 0:
                    time.sleep(wait_time)
            rest.queryArgs['partNumber'] = str(i)
            if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
                rest.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(rest.key[-32:].zfill(32))
                rest.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                    hashlib.md5(rest.key[-32:].zfill(32)).digest())
            for _ in xrange(CONFIG['PutTimesForOnePart']):
                if not fixed_size:
                    rest.contentLength, fixed_size = Util.generate_a_size(CONFIG['PartSize'])
                resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request(cal_md5=CONFIG['CalHashMD5'],
                                                                           memory_file=SHARE_MEM)
                result_queue.put(
                    (process_id, user.username, rest.recordUrl, request_type, resp.start_time, resp.end_time,
                     resp.send_bytes, resp.recv_bytes, resp.return_data, resp.request_id, resp.status, resp.id2))
                total_requests += 1
            if resp.status.startswith('200 '):
                parts_record += '%d:%s,' % (i, resp.return_data)
        upload_id.append(parts_record)
    # 记录各段信息到本地文件 ，parts_etag-x.dat，格式：桶名\t对象名\tupload_id\tpartNo:Etag,partNo:Etag,...
    part_record_file = 'data/parts_etag-%d.dat' % process_id
    parts_record_writer = None
    parts_records = ''
    for upload_id in upload_ids:
        parts_records += '\t'.join(upload_id) + '\n'
    try:
        parts_record_writer = open(part_record_file, 'w')
        parts_record_writer.write(parts_records)
    except Exception, data:
        logging.error('process [%d] write file %s error, %s' % (process_id, part_record_file, data))
    finally:
        if parts_record_writer:
            try:
                parts_record_writer.close()
            except IOError:
                pass


def copy_part(process_id, user, conn, result_queue):
    # 必须传入OBJECTS，否则无法拷贝。
    global OBJECTS
    if not OBJECTS:
        logging.error("can not find source object, exit")
        return

    # 从本地加载本进程需要做的upload_ids。考虑到单upload_id多并发上传段场景，需要加载其它进程初始化的upload_ids。
    # 如5个用户，每用户2个并发，则每个upload_id可以最大2个并发上传段。
    # upload_id-0(usr0,p0)  upload_id-1(usr0,p1)  upload_id-2(usr1,p2)  upload_id-3(usr1,p3) upload_id-4(usr2,p4)
    # upload_id-5(usr2,p5)  upload_id-6(usr3,p6)  upload_id-7(usr3,p7)  upload_id-8(usr4,p8) upload_id-9(usr4,p9)
    # p0,p1需要顺序加载usr0,p0和usr0,p1
    upload_ids = []
    if not CONFIG['ConcurrentUpParts']:
        id_files = [process_id]
    else:
        id_files = range(process_id / CONFIG['ThreadsPerUser'] * CONFIG['ThreadsPerUser'],
                         (process_id / CONFIG['ThreadsPerUser'] + 1) * CONFIG['ThreadsPerUser'])

    for i in id_files:
        upload_id_file = 'data/upload_id-%d.dat' % i
        try:
            with open(upload_id_file, 'r') as fd:
                for line in fd:
                    if line.strip() == '':
                        continue
                    # 如果非本并发的用户初始化的upload_id，跳过。
                    if not line.startswith(user.username + '\t'):
                        continue
                    if len(line.split('\t')) != 4:
                        logging.warn('upload_ids record error [%s]' % line)
                        continue
                    # 记录upload_id的原并发号i
                    upload_ids.append((str(i) + '.' + line.strip()).split('\t'))
                fd.close()
            logging.info('process %d load upload_ids file %s end' % (process_id, upload_id_file))
        except Exception, data:
            logging.error("load %s for process %d error, [%r], exit" % (upload_id_file, process_id, data))
            continue

    if not upload_ids:
        logging.warning("load no upload_ids for process %d, exit" % process_id)
        return

    fixed_size = False
    request_type = 'CopyPart'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
        rest.headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'
    if CONFIG['copySrcSrvSideEncryptType'].lower() == 'sse-c':
        rest.headers['x-amz-copy-source-server-side-encryption-customer-algorithm'] = 'AES256'
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    total_requests = 0
    parts_record = ''
    for upload_id in upload_ids:
        rest.bucket = upload_id[1]
        rest.key = upload_id[2]
        rest.queryArgs['uploadId'] = upload_id[3]
        # 如果开启了并发上传段，本并发只处理部分段。
        if not CONFIG['ConcurrentUpParts']:
            part_ids = range(1, CONFIG['PartsForEachUploadID'] + 1)
        else:
            part_ids = range(process_id % CONFIG['ThreadsPerUser'] + 1, CONFIG['PartsForEachUploadID'] + 1,
                             CONFIG['ThreadsPerUser'])
        logging.debug('process %d handle parts: %r' % (process_id, part_ids))
        if not part_ids:
            logging.warning(
                'process %d has no parts to do for upload_id %s, break' % (process_id, rest.queryArgs['uploadId']))
            continue
        for i in part_ids:
            if CONFIG['TpsPerThread']:  # 限制tps
                # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
                dst_time = total_requests * 1.0 / CONFIG['TpsPerThread'] + start_time
                wait_time = dst_time - time.time()
                if wait_time > 0:
                    time.sleep(wait_time)
            rest.queryArgs['partNumber'] = str(i)
            if not fixed_size:
                range_size, fixed_size = Util.generate_a_size(CONFIG['PartSize'])
            rest.headers['x-amz-copy-source'] = '/%s' % random.choice(OBJECTS)
            range_start_index = random.randint(0, int(CONFIG['PartSize']) - range_size)
            logging.debug('range_start_index:%d' % range_start_index)
            rest.headers['x-amz-copy-source-range'] = 'bytes=%d-%d' % (
                range_start_index, range_start_index + range_size - 1)
            logging.debug('x-amz-copy-source-range:[%s]' % rest.headers['x-amz-copy-source-range'])
            # 增加服务器端加密头域
            if CONFIG['copySrcSrvSideEncryptType'].lower() == 'sse-c':
                src_en_key = rest.headers['x-amz-copy-source'].split('/')[2][-32:].zfill(32)
                rest.headers['x-amz-copy-source-server-side-encryption-customer-key'] = base64.b64encode(src_en_key)
                rest.headers['x-amz-copy-source-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                    hashlib.md5(src_en_key).digest())
                logging.debug('src encrypt key: %s, src encrypt key md5: %s' % (
                    src_en_key, rest.headers['x-amz-copy-source-server-side-encryption-customer-key-MD5']))
            if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
                rest.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(rest.key[-32:].zfill(32))
                rest.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                    hashlib.md5(rest.key[-32:].zfill(32)).digest())

            resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
            # 同拷贝对象，若拷贝段操作先返回200 OK，并不代表拷贝成功。如果返回了200，但没有获取到ETag，将response修改为500错误。
            if resp.status.startswith('200 ') and not resp.return_data:
                logging.error('response 200 OK without ETag, set status code 500 InternalError')
                resp.status = '500 InternalError'
            result_queue.put(
                (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
                 resp.end_time, resp.send_bytes, resp.recv_bytes,
                 'src:' + rest.headers['x-amz-copy-source'] + ':' + rest.headers[
                     'x-amz-copy-source-range'], resp.request_id, resp.status, resp.id2))
            if resp.status.startswith('200 '):
                parts_record += '%d:%s,' % (i, resp.return_data)
            total_requests += 1
        upload_id.append(parts_record)
    # 记录各段信息到本地文件 ，parts_etag-x.dat，格式：桶名\t对象名\tupload_id\tpartNo:Etag,partNo:Etag,...
    part_record_file = 'data/parts_etag-%d.dat' % process_id
    parts_record_writer = None
    parts_records = ''
    for upload_id in upload_ids:
        parts_records += '\t'.join(upload_id) + '\n'
    try:
        parts_record_writer = open(part_record_file, 'w')
        parts_record_writer.write(parts_records)
    except Exception, data:
        logging.error('process [%d] write file %s error, %s' % (process_id, part_record_file, data))
    finally:
        if parts_record_writer:
            try:
                parts_record_writer.close()
            except IOError:
                pass


def complete_multi_upload(process_id, user, conn, result_queue):
    # 从本地parts_etag-x.dat中加载本进程需要做的upload_ids。考虑到单upload_id多并发上传段场景，需要加载其它进程上传的段信息。
    # 如3个用户，每用户3个并发，每个upload_id上传6个段，则每个upload_id 3个并发上传段，每个并发对每个upload_id上传2个段。
    # parts_etag-0(usr0,p0,part1/4)  parts_etag-1(usr0,p1,part2/5)  parts_etag-2(usr1,p2,part3/6)
    # parts_etag-3(usr1,p3,part1/4)  parts_etag-4(usr0,p4,part2/5)  parts_etag-5(usr1,p5,part3/6)
    # parts_etag-0(usr2,p6,part1/4)  parts_etag-1(usr0,p7,part2/5)  parts_etag-2(usr1,p8,part3/6)

    # p0,p1,p2需要顺序加载parts_etag-0, parts_etag-1, parts_etag-2,取里面属于自已的对象。

    part_etags = {}
    if not CONFIG['ConcurrentUpParts']:
        part_files = [process_id]
    else:
        part_files = range(process_id / CONFIG['ThreadsPerUser'] * CONFIG['ThreadsPerUser'],
                           (process_id / CONFIG['ThreadsPerUser'] + 1) * CONFIG['ThreadsPerUser'])

    for i in part_files:
        part_record_file = 'data/parts_etag-%d.dat' % i
        try:
            with open(part_record_file, 'r') as fd:
                for line in fd:
                    if line.strip() == '':
                        continue
                    if not line.startswith('%d.%s\t' % (process_id, user.username)):
                        continue
                    line_array = line.strip().split('\t')
                    if len(line_array) != 5 or not line_array[4]:
                        logging.warn('partEtag record error [%s]' % line)
                        continue
                    # 用户名\t桶名\t对象名\tupoadID\tpartNo:etag,partN0:etag,..
                    # 合并相同的upload_id多并发上传的段信息
                    if line_array[3] in part_etags:
                        part_etags[line_array[3]] = (
                            line_array[1], line_array[2], line_array[4] + part_etags[line_array[3]][2])
                    else:
                        part_etags[line_array[3]] = (line_array[1], line_array[2], line_array[4])
                fd.close()
            logging.debug('process %d load parts_etag file %s end' % (process_id, part_record_file))
        except Exception, data:
            logging.warning(
                "load parts_etag from file %s for process %d error, [%r], exit" % (part_record_file, process_id, data))
            continue
    if not part_etags:
        logging.error('process %d load nothing from files %r ' % (process_id, part_files))
        return
    request_type = 'CompleteMultiUpload'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.headers['content-type'] = 'application/xml'
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    total_requests = 0
    for key, value in part_etags.items():
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = total_requests * 1.0 / CONFIG['TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        rest.bucket = value[0]
        rest.key = value[1]
        rest.queryArgs['uploadId'] = key
        # 将parts信息排序
        parts_dict = {}
        for item in value[2].split(','):
            if ':' in item:
                parts_dict[int(item.split(':')[0])] = item.split(':')[1]
        # 组装xml body
        if not parts_dict:
            continue
        rest.sendContent = '<CompleteMultipartUpload>'
        for part_index in sorted(parts_dict):
            if not parts_dict[part_index]:
                continue
            rest.sendContent += '<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>' % (
                part_index, parts_dict[part_index])
        rest.sendContent += '</CompleteMultipartUpload>'
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))
        total_requests += 1


def abort_multi_upload(process_id, user, conn, result_queue):
    # 从本地加载本进程需要做的upload_ids
    upload_ids = []
    upload_id_file = 'data/upload_id-%d.dat' % process_id
    try:
        with open(upload_id_file, 'r') as fd:
            for line in fd:
                if line.strip() == '':
                    continue
                # 如果非本并发的用户初始化的upload_id，跳过。
                if not line.startswith(user.username + '\t'):
                    continue
                if len(line.split('\t')) != 4:
                    logging.warn('upload_ids record error [%s]' % line)
                    continue
                upload_ids.append(line.strip().split('\t'))
            fd.close()
        logging.info('process %d load upload_ids file %s end' % (process_id, upload_id_file))
    except Exception, data:
        logging.error("load %s for process %d error, [%r], exit" % (upload_id_file, process_id, data))
        return
    if not upload_ids:
        logging.warning("load no upload_ids for process %d, from file upload_id-%r exit" % (process_id, upload_id_file))
        return
    else:
        logging.info("total load %d upload_ids" % len(upload_ids))
    request_type = 'AbortMultiUpload'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    total_requests = 0
    for upload_id in upload_ids:
        rest.bucket = upload_id[1]
        rest.key = upload_id[2]
        rest.queryArgs['uploadId'] = upload_id[3]
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = total_requests * 1.0 / CONFIG['TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request(cal_md5=CONFIG['CalHashMD5'])
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))
        total_requests += 1


def multi_parts_upload(process_id, user, conn, result_queue):
    rest = obsPyCmd.OBSRequestDescriptor(request_type='', ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.bucket = CONFIG['BucketNameFixed']
    rest.key = CONFIG['ObjectNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    total_requests = 0
    i = 0
    while i < CONFIG['BucketsPerUser']:
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        j = 0
        while j < CONFIG['ObjectsPerBucketPerThread']:
            if not CONFIG['ObjectNameFixed']:
                if not CONFIG['ObjNamePatternHash']:
                    rest.key = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index', str(
                        j)).replace('ObjectNamePrefix',
                                    CONFIG[
                                        'ObjectNamePrefix'])
                else:
                    object_name = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                            str(
                                                                                                                j)).replace(
                        'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                    rest.key = hashlib.md5(object_name).hexdigest() + '-' + object_name
            # 1. 初始化对象多段上传任务。
            rest.requestType = 'InitMultiUpload'
            rest.method = 'POST'
            rest.headers = {}
            rest.queryArgs = {}
            rest.contentLength = 0
            rest.sendContent = ''
            rest.queryArgs['uploads'] = None
            if CONFIG['PutWithACL']:
                rest.headers['x-amz-acl'] = CONFIG['PutWithACL']
            if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
                rest.headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'
                rest.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(rest.key[-32:].zfill(32))
                rest.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                    hashlib.md5(rest.key[-32:].zfill(32)).digest())
            elif CONFIG['SrvSideEncryptType'].lower() == 'sse-kms' and CONFIG[
                'SrvSideEncryptAlgorithm'].lower() == 'aws:kms':
                rest.headers['x-amz-server-side-encryption'] = 'aws:kms'
                if CONFIG['SrvSideEncryptAWSKMSKeyId']:
                    rest.headers['x-amz-server-side-encryption-aws-kms-key-id'] = CONFIG['SrvSideEncryptAWSKMSKeyId']
                if CONFIG['SrvSideEncryptContext']:
                    rest.headers['x-amz-server-side-encryption-context'] = CONFIG['SrvSideEncryptContext']
            elif CONFIG['SrvSideEncryptType'].lower() == 'sse-kms' and CONFIG[
                'SrvSideEncryptAlgorithm'].lower() == 'aes256':
                rest.headers['x-amz-server-side-encryption'] = 'AES256'
            if CONFIG['TpsPerThread']:  # 限制tps
                # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
                dst_time = total_requests * 1.0 / CONFIG['TpsPerThread'] + start_time
                wait_time = dst_time - time.time()
                if wait_time > 0:
                    time.sleep(wait_time)
            resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
            result_queue.put(
                (process_id, user.username, rest.recordUrl, rest.requestType, resp.start_time,
                 resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))
            total_requests += 1
            upload_id = resp.return_data
            logging.info("upload id: %s" % upload_id)
            # 2. 串行上传多段
            rest.requestType = 'UploadPart'
            rest.method = 'PUT'
            rest.headers = {}
            rest.queryArgs = {}
            rest.sendContent = ''
            rest.headers['content-type'] = 'application/octet-stream'
            rest.queryArgs['uploadId'] = upload_id
            part_number = 1
            fixed_size = False
            part_etags = {}
            while part_number <= CONFIG['PartsForEachUploadID']:
                rest.queryArgs['partNumber'] = str(part_number)
                if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
                    rest.headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'
                    rest.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(
                        rest.key[-32:].zfill(32))
                    rest.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                        hashlib.md5(rest.key[-32:].zfill(32)).digest())
                if CONFIG['TpsPerThread']:  # 限制tps
                    # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
                    dst_time = total_requests * 1.0 / CONFIG['TpsPerThread'] + start_time
                    wait_time = dst_time - time.time()
                    if wait_time > 0:
                        time.sleep(wait_time)
                for _ in xrange(CONFIG['PutTimesForOnePart']):
                    if not fixed_size:
                        rest.contentLength, fixed_size = Util.generate_a_size(CONFIG['PartSize'])
                    resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request(cal_md5=CONFIG['CalHashMD5'],
                                                                               memory_file=SHARE_MEM)
                    result_queue.put(
                        (process_id, user.username, rest.recordUrl, rest.requestType, resp.start_time,
                         resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))
                    total_requests += 1
                if resp.status.startswith('200 '):
                    part_etags[part_number] = resp.return_data
                part_number += 1
            # 3. 合并段
            rest.requestType = 'CompleteMultiUpload'
            rest.method = 'POST'
            rest.headers = {}
            rest.queryArgs = {}
            rest.headers['content-type'] = 'application/xml'
            rest.queryArgs['uploadId'] = upload_id
            rest.sendContent = '<CompleteMultipartUpload>'
            for part_index in sorted(part_etags):
                rest.sendContent += '<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>' % (
                    part_index, part_etags[part_index])
            rest.sendContent += '</CompleteMultipartUpload>'
            if CONFIG['TpsPerThread']:  # 限制tps
                # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
                dst_time = total_requests * 1.0 / CONFIG['TpsPerThread'] + start_time
                wait_time = dst_time - time.time()
                if wait_time > 0:
                    time.sleep(wait_time)
            resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
            result_queue.put(
                (process_id, user.username, rest.recordUrl, rest.requestType, resp.start_time,
                 resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))
            total_requests += 1
            j += 1
        i += 1


def get_object_upload(process_id, user, conn, result_queue):
    upload_ids = []
    upload_id_file = 'data/upload_id-%d.dat' % process_id
    try:
        with open(upload_id_file, 'r') as fd:
            for line in fd:
                if line.strip() == '':
                    continue
                # 如果非本并发的用户初始化的upload_id，跳过。
                if not line.startswith(user.username + '\t'):
                    continue
                if len(line.split('\t')) != 4:
                    logging.warn('upload_ids record error [%s]' % line)
                    continue
                upload_ids.append(line.strip().split('\t'))
            fd.close()
        logging.info('process %d load upload_ids file %s end' % (process_id, upload_id_file))
    except Exception, data:
        logging.error("load %s for process %d error, [%r], exit" % (upload_id_file, process_id, data))
        return
    if not upload_ids:
        logging.warning("load no upload_ids for process %d, from file upload_id-%r exit" % (process_id, upload_id_file))
        return
    else:
        logging.info("total load %d upload_ids" % len(upload_ids))
    request_type = 'GetObjectUpload'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    total_requests = 0
    for upload_id in upload_ids:
        rest.bucket = upload_id[1]
        rest.key = upload_id[2]
        rest.queryArgs['uploadId'] = upload_id[3]
        for upload_id in upload_ids:
            rest.bucket = upload_id[1]
            rest.key = upload_id[2]
            rest.queryArgs['uploadId'] = upload_id[3]
        if CONFIG['TpsPerThread']:  # 限制tps
            # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求 / 限制TPS + 并发开始时间
            dst_time = total_requests * 1.0 / CONFIG['TpsPerThread'] + start_time
            wait_time = dst_time - time.time()
            if wait_time > 0:
                time.sleep(wait_time)
        resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request(cal_md5=CONFIG['CalHashMD5'])
        result_queue.put(
            (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
             resp.end_time, resp.send_bytes, resp.recv_bytes, '', resp.request_id, resp.status, resp.id2))


def put_object_acl(process_id, user, conn, result_queue):
    request_type = 'PutObjectAcl'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
        rest.headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'
    if CONFIG['Testcase'] in (202, 900) and CONFIG['Range']:
        rest.headers['Range'] = 'bytes=%s' % random.choice(CONFIG['Range'].split(';')).strip()
    # 如果传入OBJECTS，则直接处理OBJECTS。

    global OBJECTS
    if OBJECTS:
        handle_from_objects(request_type, rest, process_id, user, conn, result_queue)
        return

    # 如果data下有上传记录的对象名和版本，从该文件读。
    obj_v_file = 'data/objv-%d.dat' % process_id
    if os.path.exists(obj_v_file) and os.path.getsize(obj_v_file) > 0:
        handle_from_obj_v(request_type, obj_v_file, rest, process_id, user, conn, result_queue)
        return

    # 从字典序对象名下载。
    if not CONFIG['ObjectLexical']:
        logging.warn('Object name is not lexical, exit..')
        return

    i = 0
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    if CONFIG['ObjectNameFixed']:
        rest.key = CONFIG['ObjectNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    rest.queryArgs["acl"] = None
    rest.headers["x-amz-acl"] = 'public-read'
    while i < CONFIG['BucketsPerUser']:
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        j = 0
        while j < CONFIG['ObjectsPerBucketPerThread']:
            if CONFIG['TpsPerThread']:  # 限制tps
                # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求/限制TPS +　并发开始时间
                dst_time = (i * CONFIG['ObjectsPerBucketPerThread'] + j) * 1.0 / CONFIG['TpsPerThread'] + start_time
                wait_time = dst_time - time.time()
                if wait_time > 0:
                    time.sleep(wait_time)
            if CONFIG['Range']:
                rest.headers['Range'] = 'bytes=%s' % random.choice(CONFIG['Range'].split(';')).strip()

            if not CONFIG['ObjectNameFixed']:
                if not CONFIG['ObjNamePatternHash']:
                    rest.key = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index', str(
                        j)).replace('ObjectNamePrefix',
                                    CONFIG[
                                        'ObjectNamePrefix'])
                else:
                    object_name = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                            str(
                                                                                                                j)).replace(
                        'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                    rest.key = hashlib.md5(object_name).hexdigest() + '-' + object_name
            if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
                rest.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(rest.key[-32:].zfill(32))
                rest.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                    hashlib.md5(rest.key[-32:].zfill(32)).digest())

            resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request(cal_md5=CONFIG['CalHashMD5'])
            result_queue.put(
                (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
                 resp.end_time, resp.send_bytes, resp.recv_bytes, 'MD5:' + str(resp.content_md5),
                 resp.request_id, resp.status, resp.id2))
            j += 1
        i += 1


def get_object_acl(process_id, user, conn, result_queue):
    request_type = 'GetObjectAcl'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
        rest.headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'
    if CONFIG['Testcase'] in (202, 900) and CONFIG['Range']:
        rest.headers['Range'] = 'bytes=%s' % random.choice(CONFIG['Range'].split(';')).strip()
    # 如果传入OBJECTS，则直接处理OBJECTS。
    global OBJECTS
    if OBJECTS:
        handle_from_objects(request_type, rest, process_id, user, conn, result_queue)
        return

    # 如果data下有上传记录的对象名和版本，从该文件读。
    obj_v_file = 'data/objv-%d.dat' % process_id
    if os.path.exists(obj_v_file) and os.path.getsize(obj_v_file) > 0:
        handle_from_obj_v(request_type, obj_v_file, rest, process_id, user, conn, result_queue)
        return

    # 从字典序对象名下载。
    if not CONFIG['ObjectLexical']:
        logging.warn('Object name is not lexical, exit..')
        return

    i = 0
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    if CONFIG['ObjectNameFixed']:
        rest.key = CONFIG['ObjectNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    rest.queryArgs["acl"] = None
    while i < CONFIG['BucketsPerUser']:
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        j = 0
        while j < CONFIG['ObjectsPerBucketPerThread']:
            if CONFIG['TpsPerThread']:  # 限制tps
                # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求/限制TPS +　并发开始时间
                dst_time = (i * CONFIG['ObjectsPerBucketPerThread'] + j) * 1.0 / CONFIG['TpsPerThread'] + start_time
                wait_time = dst_time - time.time()
                if wait_time > 0:
                    time.sleep(wait_time)
            if CONFIG['Range']:
                rest.headers['Range'] = 'bytes=%s' % random.choice(CONFIG['Range'].split(';')).strip()
            if not CONFIG['ObjectNameFixed']:
                if not CONFIG['ObjNamePatternHash']:
                    rest.key = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index', str(
                        j)).replace('ObjectNamePrefix',
                                    CONFIG[
                                        'ObjectNamePrefix'])
                else:
                    object_name = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                            str(
                                                                                                                j)).replace(
                        'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                    rest.key = hashlib.md5(object_name).hexdigest() + '-' + object_name
            if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
                rest.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(rest.key[-32:].zfill(32))
                rest.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                    hashlib.md5(rest.key[-32:].zfill(32)).digest())

            resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request(cal_md5=CONFIG['CalHashMD5'])
            result_queue.put(
                (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
                 resp.end_time, resp.send_bytes, resp.recv_bytes, 'MD5:' + str(resp.content_md5),
                 resp.request_id, resp.status, resp.id2))
            j += 1
        i += 1


def options_object(process_id, user, conn, result_queue):
    request_type = 'OptionsObject'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    # rest.headers['Access-Control-Request-Method'] = CONFIG['AllowedMethod']
    if CONFIG['AllowedMethod']:
        if ',' in CONFIG['AllowedMethod']:
            rest.headers['Access-Control-Request-Method'] = []
            for i in CONFIG['AllowedMethod'].split(','):
                rest.headers['Access-Control-Request-Method'].append(i.upper())
        else:
            rest.headers['Access-Control-Request-Method'] = CONFIG['AllowedMethod'].upper()
    else:
        rest.headers['Access-Control-Request-Method'] = 'GET'
    rest.headers['Origin'] = CONFIG['DomainName']
    # 如果传入OBJECTS，则直接处理OBJECTS。
    global OBJECTS
    if OBJECTS:
        handle_from_objects(request_type, rest, process_id, user, conn, result_queue)
        return

    # 如果data下有上传记录的对象名和版本，从该文件读。
    obj_v_file = 'data/objv-%d.dat' % process_id
    if os.path.exists(obj_v_file) and os.path.getsize(obj_v_file) > 0:
        handle_from_obj_v(request_type, obj_v_file, rest, process_id, user, conn, result_queue)
        return

    # 从字典序对象名下载。
    if not CONFIG['ObjectLexical']:
        logging.warn('Object name is not lexical, exit..')
        return

    i = 0
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    if CONFIG['ObjectNameFixed']:
        rest.key = CONFIG['ObjectNameFixed']
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    while i < CONFIG['BucketsPerUser']:
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        j = 0
        while j < CONFIG['ObjectsPerBucketPerThread']:
            if CONFIG['TpsPerThread']:  # 限制tps
                # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求/限制TPS +　并发开始时间
                dst_time = (i * CONFIG['ObjectsPerBucketPerThread'] + j) * 1.0 / CONFIG['TpsPerThread'] + start_time
                wait_time = dst_time - time.time()
                if wait_time > 0:
                    time.sleep(wait_time)
            if CONFIG['Range']:
                rest.headers['Range'] = 'bytes=%s' % random.choice(CONFIG['Range'].split(';')).strip()
            if not CONFIG['ObjectNameFixed']:
                if not CONFIG['ObjNamePatternHash']:
                    rest.key = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index', str(
                        j)).replace('ObjectNamePrefix',
                                    CONFIG[
                                        'ObjectNamePrefix'])
                else:
                    object_name = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index',
                                                                                                            str(
                                                                                                                j)).replace(
                        'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                    rest.key = hashlib.md5(object_name).hexdigest() + '-' + object_name
            if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
                rest.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(rest.key[-32:].zfill(32))
                rest.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                    hashlib.md5(rest.key[-32:].zfill(32)).digest())

            resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request(cal_md5=CONFIG['CalHashMD5'])
            result_queue.put(
                (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
                 resp.end_time, resp.send_bytes, resp.recv_bytes, 'MD5:' + str(resp.content_md5),
                 resp.request_id, resp.status, resp.id2))
            j += 1
        i += 1


def post_object(process_id, user, conn, result_queue):
    request_type = 'PostObject'
    rest = obsPyCmd.OBSRequestDescriptor(request_type, ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'], virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'], region=CONFIG['Region'],
                                         is_http2=CONFIG['IsHTTP2'], host=conn.host)
    rest.headers['content-type'] = 'multipart/form-data; boundary=---------------------------7db143f50da2 '
    fixed_size = False
    if CONFIG['BucketNameFixed']:
        rest.bucket = CONFIG['BucketNameFixed']
    if CONFIG['ObjectNameFixed']:
        rest.key = CONFIG['ObjectNameFixed']
    if CONFIG['SrvSideEncryptType'].lower() == 'sse-kms':
        rest.headers['x-amz-server-side-encryption'] = 'aws:kms'
    elif CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
        rest.headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'
    obj_v = ''
    obj_v_file = 'data/objv-%d.dat' % process_id
    open(obj_v_file, 'w').write(obj_v)
    # 错开每个并发起始选桶，避免单桶性能瓶颈。
    range_arr = range(0, CONFIG['BucketsPerUser'])
    if CONFIG['AvoidSinBkOp']:
        range_arr = range(process_id % CONFIG['BucketsPerUser'], CONFIG['BucketsPerUser']) + range(0,
                                                                                                   process_id % CONFIG[
                                                                                                       'BucketsPerUser'])
    start_time = None
    if CONFIG['TpsPerThread']:
        start_time = time.time()  # 开始时间
    buckets_cover = 0  # 已经遍历桶数量
    for i in range_arr:
        if not CONFIG['BucketNameFixed']:
            rest.bucket = '%s.%s.%d' % (user.ak.lower(), CONFIG['BucketNamePrefix'], i)
        j = 0
        while j < CONFIG['ObjectsPerBucketPerThread']:
            if not CONFIG['ObjectNameFixed']:
                if CONFIG['ObjectLexical']:
                    rest.key = CONFIG['ObjectNamePartten'].replace('processID', str(process_id)).replace('Index', str(
                        j)).replace(
                        'ObjectNamePrefix', CONFIG['ObjectNamePrefix'])
                else:
                    object_name = Util.random_string_create(86)
                    rest.key = hashlib.md5(object_name).hexdigest() + '-' + object_name
            if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
                rest.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(rest.key[-32:].zfill(32))
                rest.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                    hashlib.md5(rest.key[-32:].zfill(32)).digest())
                logging.debug('side-encryption-customer-key: [%r]' % rest.key[-32:].zfill(32))
            put_times_for_one_obj = CONFIG['PutTimesForOneObj']
            while put_times_for_one_obj > 0:
                if CONFIG['TpsPerThread']:  # 限制tps
                    # 按限制的tps数计算当前应该到的时间。计算方法： 当前已完成的请求/限制TPS +　并发开始时间
                    dst_time = (buckets_cover * CONFIG['ObjectsPerBucketPerThread'] * CONFIG['PutTimesForOneObj'] + j *
                               CONFIG[
                                   'PutTimesForOneObj'] + (CONFIG['PutTimesForOneObj'] - put_times_for_one_obj)) * 1.0 / \
                              CONFIG['TpsPerThread'] + start_time
                    wait_time = dst_time - time.time()
                    if wait_time > 0:
                        time.sleep(wait_time)
                if not fixed_size:
                    # change size every request for the same obj.
                    rest.contentLength, fixed_size = Util.generate_a_size(CONFIG['ObjectSize'])
                put_times_for_one_obj -= 1
                rest.sendContent = '''
                        -----------------------------7db143f50da2
                        Content-Disposition: form-data; name="key"
                        %s
                        -----------------------------7db143f50da2
                        Content-Disposition: form-data; name="file"
                        Content-Type: text/plain
                        01234567890
                        Content-Disposition: form-data; name="submit"
                        Upload
                        -----------------------------7db143f50da2--
                        ''' % rest.key
                resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request(cal_md5=CONFIG['CalHashMD5'])
                result_queue.put(
                    (process_id, user.username, rest.recordUrl, request_type, resp.start_time,
                     resp.end_time, resp.send_bytes, resp.recv_bytes, 'MD5:' + str(resp.content_md5),
                     resp.request_id, resp.status, resp.id2))
                if resp.return_data:
                    obj_v += '%s\t%s\t%s\n' % (rest.bucket, rest.key, resp.return_data)
                    # 每1KB，写入对象的versionID到本地文件objv-process_id.dat
                    if len(obj_v) >= 1024:
                        logging.info('write obj_v to file %s' % obj_v_file)
                        open(obj_v_file, 'a').write(obj_v)
                        obj_v = ''
            j += 1
        buckets_cover += 1
    if obj_v:
        open(obj_v_file, 'a').write(obj_v)


# 并发进程入口
def start_process(process_id, user, test_case, results_queue, valid_start_time, valid_end_time, current_threads, lock,
                  conn=None, call_itself=False):
    global OBJECTS, CONFIG
    # 如果混合操作自身调用，不增用户，不等待。
    if not call_itself:
        lock.acquire()
        current_threads.value += 1
        lock.release()
        # 等待所有用户启动
        while True:
            # 如果时间已经被其它进程刷新，直接跳过。
            if valid_start_time.value == float(sys.maxint):
                # 若所有用户均启动，记为合法的有效开始时间
                if current_threads.value == CONFIG['Threads']:
                    valid_start_time.value = time.time() + 2
                else:
                    time.sleep(.06)
            else:
                break
        time.sleep(2)

    # 考虑混合操作重复执行场景，若已有连接，不分配连接
    if not conn:
        conn = obsPyCmd.MyHTTPConnection(host=CONFIG['OSCs'], is_secure=CONFIG['IsHTTPs'],
                                         ssl_version=CONFIG['sslVersion'], timeout=CONFIG['ConnectTimeout'],
                                         serial_no=process_id, long_connection=CONFIG['LongConnection'],
                                         conn_header=CONFIG['ConnectionHeader'], anonymous=CONFIG['Anonymous'],
                                         is_http2=CONFIG['IsHTTP2'])
    if test_case != 900:
        try:
            method_to_call = globals()[TESTCASES[test_case].split(';')[1]]
            logging.debug('method %s called ' % method_to_call.__name__)
            method_to_call(process_id, user, conn, results_queue)
        except KeyboardInterrupt:
            pass
        except Exception, e:
            import traceback
            logging.error('Call method for test case %d except: %s' % (test_case, traceback.format_exc()))
    elif test_case == 900:
        test_cases = [int(case) for case in CONFIG['MixOperations'].split(',')]
        tmp = 0
        while tmp < CONFIG['MixLoopCount']:
            logging.debug("loop count: %d " % tmp)
            tmp += 1
            for case in test_cases:
                logging.debug("case %d in mix loop called " % case)
                start_process(process_id, user, case, results_queue, valid_start_time,
                              valid_end_time, current_threads, lock, conn, True)
    # 如果混合操作自身调用，则直接返回，不断连接，不减用户。
    if call_itself:
        return

    # close connection for this thread
    if conn:
        conn.close_connection()

    # 执行完业务后，当前用户是第一个退出的用户，记为合法的结束时间
    if current_threads.value == CONFIG['Threads']:
        valid_end_time.value = time.time()
        logging.info('thread [' + str(process_id) + '], exit, set valid_end_time = ' + str(valid_end_time.value))
    # 退出
    lock.acquire()
    current_threads.value -= 1
    lock.release()
    logging.info('process_id [%d] exit, set current_threads.value = %d' % (process_id, current_threads.value))


def get_total_requests():
    global OBJECTS, CONFIG
    if CONFIG['Testcase'] == 100:
        return CONFIG['RequestsPerThread'] * CONFIG['Threads']
    elif CONFIG['Testcase'] in (
    101, 103, 104, 105, 106, 111, 112, 141, 142, 143, 151, 152, 153, 161, 162, 163, 164, 165, 167, 168, 170, 171, 173,
    174, 175, 176, 177, 178, 179, 180, 182, 185, 188):
        return CONFIG['BucketsPerUser'] * CONFIG['Users']
    elif CONFIG['Testcase'] in (201,):
        return CONFIG['ObjectsPerBucketPerThread'] * CONFIG['BucketsPerUser'] * CONFIG['Threads'] * CONFIG[
            'PutTimesForOneObj']
    elif CONFIG['Testcase'] in (202, 203, 204, 206, 207, 211, 217, 218, 219, 221, 226):
        if len(OBJECTS) > 0:
            return len(OBJECTS)
        # 如果从data下加载到对象版本数据，则不清楚总数。
        if CONFIG['Testcase'] in (202, 204):
            for i in range(CONFIG['Threads']):
                obj_v_file = 'data/objv-%d.dat' % i
                if os.path.exists(obj_v_file) and os.path.getsize(obj_v_file) > 0:
                    return -1
        return CONFIG['ObjectsPerBucketPerThread'] * CONFIG['BucketsPerUser'] * CONFIG['Threads']
    elif CONFIG['Testcase'] in (205,):
        return int((CONFIG['ObjectsPerBucketPerThread'] + CONFIG['DeleteObjectsPerRequest'] - 1) / CONFIG[
            'DeleteObjectsPerRequest']) * CONFIG['BucketsPerUser'] * CONFIG['Threads']
    elif CONFIG['Testcase'] in (216,):
        return CONFIG['ObjectsPerBucketPerThread'] * CONFIG['BucketsPerUser'] * CONFIG['Threads'] * (
            2 + CONFIG['PartsForEachUploadID'])

    # 对于某些请求无法计算请求总量，返回-1
    else:
        return -1


# return True: pass, False: failed
def precondition():
    global CONFIG, TESTCASES
    # 检查当前用户是否root用户
    import getpass
    import platform

    if 'root' != getpass.getuser() and platform.system().lower().startswith('linux'):
        return False, "\033[1;31;40m%s\033[0m Please run with root account other than '%s'" % (
            "[ERROR]", getpass.getuser())

    # 检查测试用例是否支持
    if CONFIG['Testcase'] not in TESTCASES:
        return False, "\033[1;31;40m%s\033[0m Test Case [%d] not supported" % ("[ERROR]", CONFIG['Testcase'])

    # 如果开启服务器端加密功能，必须使用https+AWSV4
    if CONFIG['SrvSideEncryptType']:
        if not CONFIG['IsHTTPs']:
            CONFIG['IsHTTPs'] = True
            logging.warning('change IsHTTPs to True while use SrvSideEncryptType')
        if CONFIG['AuthAlgorithm'] != 'AWSV4':
            CONFIG['AuthAlgorithm'] = 'AWSV4'
            logging.warning('change AuthAlgorithm to AWSV4 while use SrvSideEncryptType')

    # 加载用户,检查user是否满足要求
    logging.info('loading users...')
    read_users()
    if CONFIG['Users'] > len(USERS):
        return False, "\033[1;31;40m%s\033[0m Not enough users in users.dat after index %d: %d < [Users=%d]" % (
            "[ERROR]", CONFIG['UserStartIndex'], len(USERS), CONFIG['Users'])

    # 测试网络连接
    if CONFIG['IsHTTPs']:
        try:
            import ssl as ssl

            if not CONFIG['sslVersion']:
                CONFIG['sslVersion'] = 'SSLv23'
            logging.info('import ssl module done, config ssl Version: %s' % CONFIG['sslVersion'])
        except ImportError:
            logging.warning('import ssl module error')
            return False, 'Python version %s ,import ssl module error' % sys.version.split(' ')[0]
    oscs = CONFIG['OSCs'].split(',')
    for end_point in oscs:
        print 'Testing connection to %s\t' % end_point.ljust(20),
        sys.stdout.flush()
        test_conn = None
        try:
            test_conn = obsPyCmd.MyHTTPConnection(host=end_point, is_secure=CONFIG['IsHTTPs'],
                                                  ssl_version=CONFIG['sslVersion'], timeout=60, serial_no=0,
                                                  is_http2=CONFIG['IsHTTP2'])
            test_conn.create_connection()
            test_conn.connect_connection()
            ssl_ver = ''
            if CONFIG['IsHTTPs'] and not CONFIG['IsHTTP2']:
                if Util.compare_version(sys.version.split()[0], '2.7.9') < 0:
                    ssl_ver = test_conn.connection.sock._sslobj.cipher()[1]
                else:
                    ssl_ver = test_conn.connection.sock._sslobj.version()
                rst = '\033[1;32;40mSUCCESS  %s\033[0m'.ljust(10) % ssl_ver
            else:
                rst = '\033[1;32;40mSUCCESS\033[0m'.ljust(10)
            print rst
            logging.info(
                'connect %s success, python version: %s,  ssl_ver: %s' % (
                    end_point, sys.version.replace('\n', ' '), ssl_ver))
        except Exception, data:
            logging.error('Caught exception when testing connection with %s, except: %s' % (end_point, data))
            print '\033[1;31;40m%s *%s*\033[0m' % (' Failed'.ljust(8), data)
            return False, 'Check connection failed'
        finally:
            if test_conn:
                test_conn.close_connection()

    # 创建data目录
    if not os.path.exists('data'):
        os.mkdir('data')

    if not os.path.exists('position'):
        os.mkdir('position')

    return True, 'check passed'


def get_objects_from_file(file_name):
    global OBJECTS
    if not os.path.exists(file_name):
        print 'ERROR，the file configured %s in config.dat not exist' % file_name
        sys.exit(0)
    try:
        with open(file_name, 'r') as fd:
            for line in fd:
                if line.strip() == '':
                    continue
                if len(line.split(',')) != 13:
                    continue
                if line.split(',')[2][1:].find('/') == -1:
                    continue
                if line.split(',')[11].strip().startswith('200'):
                    OBJECTS.append(line.split(',')[2][1:])
            fd.close()
        logging.warning('load file %s end, get objects [%d]' % (file_name, len(OBJECTS)))
    except Exception, data:
        msg = 'load file %s except, %s' % (file_name, data)
        logging.error(msg)
        print msg
        sys.exit()
    if len(OBJECTS) == 0:
        print 'get no objects in file %s' % file_name
        sys.exit()


# running config
CONFIG = {}
# test users
USERS = []
OBJECTS = []
# initialize a shared memory file with fixed size: 1M
SHARE_MEM = create_file_in_memory()

APPEND_OBJECTS = {}

LIST_INDEX = []

TESTCASES = {100: 'ListUserBuckets;list_user_buckets',
             101: 'CreateBucket;create_bucket',
             102: 'ListObjectsInBucket;list_objects_in_bucket',
             103: 'HeadBucket;head_bucket',
             104: 'DeleteBucket;delete_bucket',
             105: 'BucketDelete;bucket_delete',
             106: 'OptionsBucket;options_bucket',
             111: 'PutBucketVersiong;put_bucket_versioning',
             112: 'GetBucketVersioning;get_bucket_versioning',
             141: 'PutBucketWebsite;put_bucket_website',
             142: 'GetBucketWebsite;get_bucket_website',
             143: 'DeleteBucketWebsite;delete_bucket_website',
             151: 'PutBucketCors;put_bucket_cors',
             152: 'GetBucketCors;get_bucket_cors',
             153: 'DeleteBucketCors;delete_bucket_cors',
             161: 'PutBucketTag;put_bucket_tag',
             162: 'GetBucketTag;get_bucket_tag',
             163: 'DeleteBucketTag;delete_bucket_tag',
             164: 'PutBucketLog;put_bucket_log',
             165: 'GetBucketLog;get_bucket_log',
             167: 'PutBucketStorageQuota;put_bucket_storage_quota',
             168: 'GetBucketStorageQuota;get_bucket_storage_quota',
             170: 'PutBucketAcl;put_bucket_acl',
             171: 'GetBucketAcl;get_bucket_acl',
             173: 'PutBucketPolicy;put_bucket_policy',
             174: 'GetBucketPolicy;get_bucket_policy',
             175: 'DeleteBucketPolicy;delete_bucket_policy',
             176: 'PutBucketLifecycle;put_bucket_lifecycle',
             177: 'GetBucketLifecycle;get_bucket_lifecycle',
             178: 'DeleteBucketLifecycle;delete_bucket_lifecycle',
             179: 'PutBucketNotification;put_bucket_notification',
             180: 'GetBucketNotification;get_bucket_notification',
             182: 'GetBucketMultiPartsUpload;get_bucket_multi_parts_upload',
             185: 'GetBucketLocation;get_bucket_location',
             188: 'GetBucketStorageInfo;get_bucket_storageinfo',
             201: 'PutObject;put_object',
             202: 'GetObject;get_object',
             203: 'HeadObject;head_object',
             204: 'DeleteObject;delete_object',
             205: 'DeleteMultiObjects;delete_multi_objects',
             206: 'CopyObject;copy_object',
             207: 'RestoreObject;restore_object',
             208: 'AppendObject;append_object',
             209: 'ImageProcess;image_process',
             211: 'InitMultiUpload;init_multi_upload',
             212: 'UploadPart;upload_part',
             213: 'CopyPart;copy_part',
             214: 'CompleteMultiUpload;complete_multi_upload',
             215: 'AbortMultiUpload;abort_multi_upload',
             216: 'MultiPartsUpload;multi_parts_upload',
             217: 'GetObjectUpload;get_object_upload',  # 需先执行InitMultiUpload
             218: 'PutObjectAcl;put_object_acl',
             219: 'GetObjectAcl;get_object_acl',
             221: 'OptionsObject;options_object',
             226: 'PostObject;post_object',
             900: 'MixOperation;'
             }


def generate_run_header(mode):
    """
    generate tool running header
    :param mode: running mode
    :return: version
    """
    mode = '------------------------Mode: %s----------------------------' % mode
    logging.warning(VERSION)
    logging.warning(mode)
    print VERSION, mode
    print 'Config loaded'

    return VERSION


def generate_distributed_mode_information(master, slaves):
    """
    
    :param master: 
    :param slaves: 
    :return: None 
    """
    print "Role         IP"
    print "%s       %s" % (Role.MASTER, master)
    for slave in slaves:
        print "%s        %s" % (Role.SLAVE, slave.localIP)


def run_in_distributed_mode(mode):
    """
    run obscmdbench in distributed mode
    :param mode: running mode
    :return: None
    """
    if not os.path.exists('result'):
        os.mkdir('result')

    master_path = os.getcwd() + '/result/'

    # 初始化运行工具的版本的模式
    version = generate_run_header(mode)

    # 加载指定配置文件
    logging.info('loading distributed mode config...')
    distribute_config_file = 'distribute_config.dat'

    # 获取distribute_config.dat所有相关配置
    distribute_config = Util.read_distribute_config(distribute_config_file)

    # 获取子服务器信息并建立连接
    slaves = Util.generate_slave_servers(distribute_config)
    connects = Util.generate_connections(slaves)

    # 打印所提供的master和slaves服务器信息
    generate_distributed_mode_information(distribute_config['Master'], slaves)

    threads = []

    for connect in connects:
        t = threading.Thread(target=Util.start_tool, args=(connect, CONFIG['Testcase'],
                                                           int(distribute_config['RunTime']),))
        threads.append(t)

    for thread in threads:
        thread.start()

    print "\nAll threads started..."

    for thread in threads:
        thread.join()

    time.sleep(int(distribute_config['RunTime']) + 10)

    print "Close old connections"
    for connect in connects:
        connect.close()

    print "Start to collect data from slaves..."

    file_line_number_list = []
    tps_list = []
    avg_latency_list = []
    requests_list = []
    requests_ok_list = []
    run_time_list = []
    send_bytes_list = []
    recv_bytes_list = []
    result_file = time.strftime('result/%Y.%m.%d_%H.%M.%S', time.localtime()) + '_distributed_result.txt'
    report_writer = open(result_file, 'w')

    report_writer.write('\n*****************Result****************\n')

    # start collecting brief data
    logging.warn("start to collect data from slave servers")
    logging.warn("build new connections")
    new_connects = Util.generate_connections(slaves)
    for connect in new_connects:
        slave_brief_file_name = Util.get_brief_file_name(connect)
        copy_slave_brief_to_master_cmd = r"scp `ls -t result/*_brief.txt | head -1` root@%s:%s%s" % (distribute_config['Master'], master_path, slave_brief_file_name + '[' + connect.ip + ']')
        connect.execute_cmd(copy_slave_brief_to_master_cmd, expect_end="password", timeout=10)
        connect.execute_cmd(connect.password, timeout=10)
        tps = connect.execute_cmd(r"grep '\[TPS\]' `ls -t result/*_brief.txt | head -1` | awk '{print $2}'")
        avg_latency = connect.execute_cmd(
            r"grep '\[AvgLatency\]' `ls -t result/*_brief.txt | head -1` | awk '{print $2}'")
        requests = connect.execute_cmd(r"grep '\[Requests\]' `ls -t result/*_brief.txt | head -1` | awk '{print $2}'")
        requests_ok = connect.execute_cmd(r"grep '\[OK\]' `ls -t result/*_brief.txt | head -1` | awk '{print $2}'")
        run_time = connect.execute_cmd(r"grep 'runTime' `ls -t result/*_brief.txt | head -1` | awk '{print $2}'")
        total_send_bytes = connect.execute_cmd(
            r"grep 'roughTotalSendBytes' `ls -t result/*_brief.txt | head -1` | awk '{print $2}'")
        total_send_bytes = total_send_bytes.split('\r\n')
        total_recv_bytes = connect.execute_cmd(
            r"grep 'roughTotalRecvBytes' `ls -t result/*_brief.txt | head -1` | awk '{print $2}'")
        total_recv_bytes = total_recv_bytes.split('\r\n')
        tps_list.append(float(Util.generate_response(tps)))
        avg_latency_list.append(float(Util.generate_response(avg_latency)))
        requests_list.append(int(Util.generate_response(requests)))
        requests_ok_list.append(int(Util.generate_response(requests_ok)))
        run_time = Util.generate_response(run_time)
        run_time_list.append(float(run_time))
        send_bytes_list.append(int(total_send_bytes[0]))
        recv_bytes_list.append(int(total_recv_bytes[0]))

    # close connections
    for connect in connects:
        connect.close()

    # prepare the data
    # Util.create_result_folder()
    tps = sum(requests_ok_list) / min(run_time_list)
    avg_latency = sum(avg_latency_list) / len(avg_latency_list)
    requests = sum(requests_list)
    requests_ok = sum(requests_ok_list)
    total_send_bytes = sum(send_bytes_list)
    total_recv_bytes = sum(recv_bytes_list)
    send_through_put = Util.convert_to_size_str(total_send_bytes / min(run_time_list)) + '/s'
    recv_through_put = Util.convert_to_size_str(total_recv_bytes / min(run_time_list)) + '/s'

    run_time = '%-52s' % Util.convert_time_format_str(min(run_time_list))
    error_rate_without_format = 100.0 * ((float(requests) - float(requests_ok)) / float(requests))
    error_rate_formatted = '%.2f %% ' % error_rate_without_format

    total_result = '[RunTime]        ' + run_time + '\n' + \
                   '[Requests]       ' + str(requests) + '\n' + \
                   '[OK]             ' + str(requests_ok) + '\n' + \
                   '[TPS]            ' + str(tps) + '\n' + \
                   '[AvgLatency]     ' + str(avg_latency) + '\n' + \
                   '[ErrorRate]      ' + str(error_rate_formatted) + '\n' + \
                   '[DataSend]       ' + Util.convert_to_size_str(total_send_bytes) + '\n' + \
                   '[DataRecv]       ' + Util.convert_to_size_str(total_recv_bytes) + '\n' + \
                   '[SendThroughput] ' + send_through_put + '\n' + \
                   '[RecvThroughput] ' + recv_through_put + '\n'

    if CONFIG["PrintProgress"]:
        print "\n***Total result***"
        print total_result
    report_writer.write('\n*************************Result in brief*************************\n' + total_result + '\n')
    report_writer.close()

    time.sleep(2)
    print "Your test data has been successfully saved to file: %s" % result_file
    print "\nwaiting to exit..."
    time.sleep(2)
    print version


def check_connection(server, file_name):
    """
    
    :param server: 
    :param file_name: 
    :return: 
    """
    while True:
        start_time = int(round(time.time() * 1000))
        command = r"curl --connect-timeout 8 -m 30 http://%s:5080 -v " % server + " > /dev/null 2>&1; echo $?"
        result = commands.getstatusoutput(command)
        end_time = int(round(time.time() * 1000))
        used_time = end_time - start_time
        if result[0] != 0:
            line = str(result[0]) + ',' + str(start_time) + ',' + str(end_time) + ',' + str(used_time)
            os.system(r"echo '%s' >> %s 2>&1" % (line, file_name))
            time.sleep(1)


def run_connection_checker():
    """
    
    :return: 
    """
    logging.warn("start to curl...")
    oscs = CONFIG['OSCs'].split(',')

    thread_list = []
    for server in oscs:
        file_name = time.strftime('result/%Y.%m.%d_%H.%M.%S', time.localtime()) + '_' + TESTCASES[CONFIG['Testcase']].split(';')[0].split(';')[0] + '_' + str(int(CONFIG['Users']) * int(CONFIG['ThreadsPerUser'])) + '_curl_' + server + '.txt'
        t = threading.Thread(target=check_connection, args=(server, file_name, ))
        t.daemon = True
        thread_list.append(t)

    for t in thread_list:
        t.start()


def run_in_integrated_mode(mode):
    """
    run obscmdbench in integrated mode, this is how we used obscmdbench before
    :param mode: running mode
    :return: None
    """
    # 初始化运行工具的版本及模式
    version = generate_run_header(mode)

    # 处理获取到的配置
    print str(CONFIG).replace('\'', '')
    logging.info(CONFIG)

    # 启动前检查
    check_result, msg = precondition()
    if not check_result:
        print 'Check error, [%s] \nExit...' % msg
        sys.exit()

    if CONFIG['objectDesFile']:
        # 判断操作类型，其它操作不预读文件，即使配置了objectDesFile
        obj_op = ['202', '203', '204', '213']
        if str(CONFIG['Testcase']) in obj_op or (
                        CONFIG['Testcase'] == 900 and (set(CONFIG['MixOperations'].split(',')) & set(obj_op))):
            print 'begin to read object file %s' % CONFIG['objectDesFile']
            get_objects_from_file(CONFIG['objectDesFile'])
            print 'finish, get %d objects' % len(OBJECTS)
    start_wait = False
    if start_wait:
        tip = '''
     --------------------------------------------------------------------------------
      Important: This is the way how we can run multi-clients at the same time.
      Assuming all the client nodes are sync with the time server.
      If now 02:10:00, enter 12 to change the minute, then it will start at 02:12:00
     --------------------------------------------------------------------------------
    '''
        print '\033[1;32;40m%s\033[0m' % tip

        def input_func(input_data):
            input_data['data'] = raw_input()

        while False:
            n = datetime.datetime.now()
            print 'Now it\'s %2d:\033[1;32;40m%2d\033[0m:%2d, please input to change the minute' % (
                n.hour, n.minute, n.second),
            print '(Press \'Enter\' or wait 30 sec to run, \'q\' to exit): ',
            try:
                input_data = {'data': 'default'}
                t = threading.Thread(target=input_func, args=(input_data,))
                t.daemon = True
                t.start()
                t.join(30)  # 等待30秒
                if input_data['data'] == 'q':
                    sys.exit()
                elif '' == input_data['data'] or 'default' == input_data['data']:
                    break
                try:
                    input_data['data'] = int(input_data['data'])
                except ValueError:
                    print '[ERROR] I only receive numbers (*>﹏<*)'
                    continue
                n = datetime.datetime.now()
                diff = input_data['data'] * 60 - (n.minute * 60 + n.second)
                if diff > 0:
                    print 'Wait for %d seconds...' % diff
                    time.sleep(diff)
                    break
                else:
                    break
            except KeyboardInterrupt:
                print '\nSystem exit...'
                sys.exit()
    msg = 'Start at %s, pid:%d. Press Ctr+C to stop. Screen Refresh Interval: 3 sec' % (
        time.strftime('%X %x %Z'), os.getpid())
    print msg
    logging.warning(msg)
    # valid_start_time: 所有并发均启动。
    # valid_end_time: 第一个并发退出时刻。
    # current_threads：当前运行的并发数。-2表示手动退出，-1表示正常退出。
    global valid_start_time
    valid_start_time = multiprocessing.Value('d', float(sys.maxint))
    valid_end_time = multiprocessing.Value('d', float(sys.maxint))
    current_threads = multiprocessing.Value('i', 0)
    # results_queue, 请求记录保存队列。多进程公用。
    results_queue = multiprocessing.Queue(0)

    # 启动统计计算结果的进程 。用于从队列取请求记录，保存到本地，并同时刷新实时结果。
    results_writer = results.ResultWriter(CONFIG, TESTCASES[CONFIG['Testcase']].split(';')[0].split(';')[0],
                                          results_queue, get_total_requests(),
                                          valid_start_time, valid_end_time, current_threads)
    results_writer.daemon = True
    results_writer.name = 'resultsWriter'
    results_writer.start()
    print 'resultWriter started, pid: %d' % results_writer.pid
    # 增加该进程的优先级
    os.system('renice -19 -p ' + str(results_writer.pid) + ' >/dev/null 2>&1')
    time.sleep(.2)

    if CONFIG['TestNetwork']:
        run_connection_checker()

    # 顺序启动多个业务进程
    process_list = []
    # 多进程公用锁
    lock = multiprocessing.Lock()
    esc = chr(27)  # escape key
    i = 0
    conn = None
    if CONFIG['IsHTTP2'] and CONFIG['IsShareConnection']:
        # http2 可以复用链接发送多个请求，此时conn共用
        conn = obsPyCmd.MyHTTPConnection(host=CONFIG['OSCs'], is_secure=CONFIG['IsHTTPs'],
                                         ssl_version=CONFIG['sslVersion'], timeout=CONFIG['ConnectTimeout'],
                                         long_connection=CONFIG['LongConnection'],
                                         conn_header=CONFIG['ConnectionHeader'], anonymous=CONFIG['Anonymous'],
                                         is_http2=CONFIG['IsHTTP2'])

    while i < CONFIG['Threads']:
        p = multiprocessing.Process(target=start_process, args=(
            i, USERS[i / CONFIG['ThreadsPerUser']], CONFIG['Testcase'], results_queue, valid_start_time, valid_end_time,
            current_threads,
            lock, conn,
            False))
        i += 1
        p.daemon = True
        p.name = 'worker-%d' % i
        p.start()
        # 将各工作进程的优先级提高1
        os.system('renice -1 -p ' + str(p.pid) + ' >/dev/null 2>&1')
        process_list.append(p)

    logging.info('All %d threads started, valid_start_time: %.3f' % (len(process_list), valid_start_time.value))

    # 请求未完成退出
    def exit_force(signal_num, e):
        msg = "\n\n\033[5;33;40m[WARN]Terminate Signal %d Received. Terminating... please wait\033[0m" % signal_num
        logging.warn('%r' % msg)
        print msg, '\nWaiting for all the threads exit....'
        lock.acquire()
        current_threads.value = -2
        lock.release()
        time.sleep(.1)
        tmpi = 0
        for j in process_list:
            if j.is_alive():
                if tmpi >= 100:
                    logging.warning('force to terminate process %s' % j.name)
                    j.terminate()
                else:
                    time.sleep(.1)
                    tmpi += 1
                    break

        print "\033[1;32;40mWorkers exited.\033[0m Waiting curl checker exit...",
        os.system(r"kill \-9 `pgrep curl`")
        print "\033[1;32;40m[WARN] Terminated\033[0m\n"

        print "\033[1;32;40mWorkers exited.\033[0m Waiting results_writer exit...",
        sys.stdout.flush()
        while results_writer.is_alive():
            current_threads.value = -2
            tmpi += 1
            if tmpi > 1000:
                logging.warn('retry too many times, shutdown results_writer using terminate()')
                results_writer.generate_write_final_result()
                results_writer.terminate()
            time.sleep(.01)
        print "\n\033[1;33;40m[WARN] Terminated\033[0m\n"

        print version
        sys.exit()

    import signal

    signal.signal(signal.SIGINT, exit_force)
    signal.signal(signal.SIGTERM, exit_force)

    time.sleep(1)
    # 正常退出
    stop_mark = False
    while not stop_mark:
        time.sleep(.3)
        if CONFIG['RunSeconds'] and (time.time() - valid_start_time.value >= CONFIG['RunSeconds']):
            logging.warn('time is up, exit')
            results_writer.generate_write_final_result()
            exit_force(99, None)
        for j in process_list:
            if j.is_alive():
                break
            stop_mark = True
    for j in process_list:
        j.join()
    # 等待结果进程退出。
    logging.info('Waiting results_writer to exit...')
    print "\033[1;32;40mWorkers exited.\033[0m Waiting curl checker exit...",
    os.system(r"kill \-9 `pgrep curl`")

    print "\033[1;32;40m[WARN] Terminated\033[0m\n"
    while results_writer.is_alive():
        current_threads.value = -1  # inform results_writer
        time.sleep(.3)
    print "\n\033[1;33;40m[WARN] Terminated after all requests\033[0m\n"
    print version


if __name__ == '__main__':
    if not os.path.exists('log'):
        os.mkdir('log')
    logging.config.fileConfig('logging.conf')
    # 加载指定配置文件
    logging.info('loading config...')
    config_file = 'config.dat'
    if len(sys.argv[1:]) > 2:
        config_file = sys.argv[1:][2]

    # 获取config.dat所有相关配置，并写入全局变量CONFIG
    read_config(config_file)

    # 如果携带参数，则使用参数，覆盖配置文件。
    if len(sys.argv[1:]) > 0:
        CONFIG['Testcase'] = int(sys.argv[1:][0])

    if CONFIG['Testcase'] == 209 or (CONFIG['Testcase'] == 900 and '209' in CONFIG['MixOperations']):
        generate_image_process_parameters()

    if len(sys.argv[1:]) > 1:
        CONFIG['Users'] = int(sys.argv[1:][1])
        CONFIG['Threads'] = CONFIG['Users'] * CONFIG['ThreadsPerUser']

    # 如果是追加写请求208，需要提前加载对象Position
    if CONFIG['Testcase'] == 208:
        APPEND_OBJECTS = generate_append_object_position()

    # 将对象编号写入列表
    if is_needed_to_build_list_index():
        initialize_object_index()

    # 判断运行模式
    if CONFIG['Mode'] == '1' or not CONFIG['IsMaster']:
        # integrated mode, execute obscmdbench like before
        run_in_integrated_mode(Mode.INTEGRATED)
    elif CONFIG['Mode'] == '2' and CONFIG['IsMaster']:
        run_in_distributed_mode(Mode.DISTRIBUTED)
