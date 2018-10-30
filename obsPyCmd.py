# -*- coding:utf-8 -*-
import Util
import time
import logging
import httplib
import urllib
import random
import hashlib
import os
import sys
import re
import copy
import base64
import hmac
from urlparse import urlparse
import AuthorizationHandler

if sys.version < '2.7':
    import myLib.myhttplib as httplib
try:
    import ssl
except ImportError:
    logging.warning('import ssl module error')
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Legacy Python that doesn't verify HTTPS certificates by default
    logging.warning('create unverified https context except')
else:
    # Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context


# 根据序号初始化一个标准的HTTP连接
class MyHTTPConnection:
    def __init__(self, host, is_secure=False, ssl_version=None, timeout=80, serial_no=0, long_connection=False,
                 conn_header='', anonymous=False, is_http2=False):
        self.isSecure = is_secure
        self.anonymous = anonymous
        if self.isSecure:
            self.sslVersion = ssl.__dict__['PROTOCOL_' + ssl_version]
        self.timeout = timeout
        self.connection = None
        self.host = host.split(',')[serial_no % len(host.split(','))]
        self.root_host = self.host
        self.longConnection = long_connection
        self.conn_header = conn_header
        self.is_http2 = is_http2

    def create_connection(self):
        if self.isSecure:
            if not self.is_http2:
                if Util.compare_version(sys.version.split()[0], '2.7.9') >= 0:
                    self.connection = httplib.HTTPSConnection(self.host + ':443', timeout=self.timeout,
                                                              context=ssl.SSLContext(self.sslVersion))
                else:
                    self.connection = httplib.HTTPSConnection(self.host + ':443', timeout=self.timeout)
            else:
                import http2
                context = ssl.SSLContext(self.sslVersion)
                context.set_alpn_protocols(['h2'])
                # self.connection = HTTP20Connection(self.host, port=443, ssl_context=context, secure=True)
                self.connection = http2.HTTP20ConnectionWrapper(self.host, port=443, ssl_context=context, secure=True)
        elif not self.is_http2:
            self.connection = httplib.HTTPConnection(self.host + ':80', timeout=self.timeout)
        else:
            import http2
            # 使用http2.0
            self.connection = http2.HTTP20ConnectionWrapper(self.host, port=80)
            # self.connection = HTTP20Connection(self.host, port=80)
        logging.debug('create connection to host: ' + self.host)

    def close_connection(self):
        if not self.connection:
            return
        try:
            self.connection.close()
        except Exception, data:
            logging.error('Caught [%s], when close a connection' % data)
            # 此处暂不抛异常
            pass
        finally:
            self.connection = None

    def connect_connection(self):
        self.connection.connect()


class OBSRequestDescriptor:
    def __init__(self, request_type, ak='', sk='', auth_algorithm='', bucket="", key="", send_content='',
                 content_length=0, virtual_host=False, domain_name='obs.huawei.com', region='dftRgn', is_cdn=False,
                 cdn_ak='', cdn_sk='', cdn_sts_token='', is_data_from_file=False, local_file_path=None,  is_http2=False,
                 host=None):
        self.requestType = request_type
        self.ak = ak
        self.sk = sk
        self.AuthAlgorithm = auth_algorithm
        self.bucket = bucket
        self.key = key
        self.sendContent = send_content
        self.contentLength = content_length
        self.virtualHost = virtual_host
        self.domainName = domain_name
        self.region = region
        self.url = ''
        self.recordUrl = ''  # 记录到detail文件中的url
        self.headers = {}
        self.queryArgs = {}
        self.method = self._get_http_method_from_request_type_()
        self.is_cdn = is_cdn
        self.cdn_ak = cdn_ak
        self.cdn_sk = cdn_sk
        self.cdn_sts_token = cdn_sts_token
        self.is_data_from_file = is_data_from_file
        self.local_file_path = local_file_path
        self.is_http2 = is_http2
        self.host = host

    def _get_http_method_from_request_type_(self):
        if self.requestType in (
                'ListUserBuckets', 'ListObjectsInBucket', 'GetObject', 'GetBucketVersioning', 'GetBucketWebsite',
                'GetBucketCORS', 'GetBucketTag', 'GetBucketLog', 'GetBucketStorageQuota', 'GetBucketAcl',
                'GetBucketPolicy', 'GetBucketLifecycle', 'GetBucketNotification', 'GetBucketMultiPartsUpload',
                'GetBucketLocation', 'GetBucketStorageInfo', 'GetObjectUpload', 'GetObjectAcl', 'ImageProcess'
        ):
            return 'GET'
        elif self.requestType in (
                'CreateBucket', 'PutObject', 'PutBucketVersioning', 'PutBucketWebsite', 'UploadPart', 'CopyPart',
                'CopyObject', 'PutBucketCORS', 'PutBucketTag', 'PutBucketLog', 'PutBucketStorageQuota', 'PutBucketAcl',
                'PutBucketPolicy', 'PutBucketLifecycle', 'PutBucketNotification', 'PutObjectAcl'
        ):
            return 'PUT'
        elif self.requestType in ('HeadBucket', 'HeadObject'):
            return 'HEAD'
        elif self.requestType in (
                'DeleteBucket', 'DeleteObject', 'DeleteBucketWebsite', 'DeleteBucketCORS', 'AbortMultiUpload',
                'DeleteBucketTag', 'DeleteBucketPolicy', 'DeleteBucketLifecycle'
        ):
            return 'DELETE'
        elif self.requestType in (
                'BucketDelete', 'RestoreObject', 'DeleteMultiObjects', 'InitMultiUpload', 'CompleteMultiUpload',
                'PostObject', 'AppendObject'
        ):
            return 'POST'
        elif self.requestType in ('OPTIONSBucket', 'OptionsObject'):
            return 'OPTIONS'
        else:
            return ''

    # 以下为外部允许调用的接口

    def generate_url(self):
        # 先初始化为''
        self.url = ''
        # 根据virtualHost，桶，对象生成url
        if self.bucket and (not self.virtualHost):
            self.url = '/%s' % self.bucket
        self.url += "/%s" % urllib.quote_plus(self.key)
        # 将参数加入url
        for key in sorted(self.queryArgs):
            if self.queryArgs[key] and self.queryArgs[key].strip():
                if self.url.find('?') != -1:
                    self.url += ('&' + key + '=' + urllib.quote_plus(self.queryArgs[key]))
                else:
                    self.url += ('?' + key + '=' + urllib.quote_plus(self.queryArgs[key]))

            elif self.queryArgs[key] is None or self.queryArgs[key].strip() == '':
                if self.url.find('?') != -1:
                    self.url += ('&' + key)
                else:
                    self.url += ('?' + key)
        if self.bucket and self.virtualHost:
            self.recordUrl = '%s:%s' % (self.bucket, self.url)  # 虚拟主机方式记录url加前缀bucket:
        else:
            self.recordUrl = self.url
        logging.debug('generate url ended, [%s]' % self.url)

    def add_content_length_header(self):
        # send_content  content_length 传入可能不匹配。
        # 若sendContent不为空，则不管contentLength是否为0，刷新contentLength为sendContent长度。
        # 若sendContent为空，但contentLength为不0，系统保持contentLength 不变，在后续请求时从内存随机生成指定大小的对象。
        # 若sendContent为空，同时contentLength为0，不处理。
        if self.sendContent:
            self.contentLength = self.headers['Content-Length'] = len(self.sendContent)
        elif self.sendContent == '' and self.contentLength != 0:
            self.headers['Content-Length'] = self.contentLength
        elif self.sendContent == '' and self.contentLength == 0:
            self.headers['Content-Length'] = 0

    def add_host_header(self, hostname=None):
        if hostname:
            self.headers['Host'] = hostname
        else:
            if not self.virtualHost:
                self.headers['Host'] = self.host
            elif self.bucket:
                self.headers['Host'] = self.bucket + '.' + self.domainName
            else:
                self.headers['Host'] = self.domainName

        logging.debug('add host header: %s' % self.headers['Host'])


class DefineResponse:
    def __init__(self):
        self.status = ''
        self.request_id = '9999999999999999'
        self.id2 = ''
        self.start_time = time.time()
        self.end_time = 0.0
        self.send_bytes = 0
        self.recv_bytes = 0
        self.return_data = None
        self.content_md5 = ''
        self.position = '0'  # 普通对象上传Position默认为-1

    @property
    def to_string(self):
        return 'request_id: %s, status: %s,  return_data: %r, start_time: %.3f, end_time: %.3f, sendBytes: %d, ' \
               'recvBytes: %d, content_md5: %s, x-amz-id-2: %s, x-obs-next-append-position: %s' % (self.request_id, self.status, self.return_data,
                                                                   self.start_time, self.end_time, self.send_bytes,
                                                                   self.recv_bytes, self.content_md5, self.id2, self.position)


class OBSRequestHandler:
    def __init__(self, obs_request, my_http_connection):
        self.obsRequest = obs_request
        # 如果未传入连接（短连接），则重新建立连接。
        self.myHTTPConnection = my_http_connection
        self._init_connection_()
        self.myCopyHTTPConnection = None  # 用于重定向的连接。
        # 为obsRequest刷新url 和时间签名头域
        self.obsRequest.generate_url()
        self.obsRequest.add_content_length_header()
        self.obsRequest.add_host_header()

        if not my_http_connection.anonymous:
            try:
                # handle auth
                if self.obsRequest.AuthAlgorithm.lower() == 'awsv2':
                    AuthorizationHandler.HmacAuthV2Handler(self.obsRequest).handle()
                elif self.obsRequest.AuthAlgorithm.lower() == 'awsv4':
                    AuthorizationHandler.HmacAuthV4Handler(self.obsRequest).handle()
                elif random.randint(0, 1):
                    AuthorizationHandler.HmacAuthV4Handler(self.obsRequest).handle()
                else:
                    AuthorizationHandler.HmacAuthV2Handler(self.obsRequest).handle()
            except Exception, data:
                import traceback
                stack = traceback.format_exc()
                logging.error('add authorization exception, %s\n%s' % (data, stack))

        # 初始化一个请求的结果
        self.defineResponse = DefineResponse()

    def _init_connection_(self):
        # 如果连接为空，则用新的host创建链接
        if not self.myHTTPConnection.connection:
            if self.obsRequest.bucket and self.obsRequest.virtualHost:
                    self.myHTTPConnection.host = self.obsRequest.bucket + '.' + self.myHTTPConnection.root_host
            else:
                self.myHTTPConnection.host = self.myHTTPConnection.root_host
            self.myHTTPConnection.create_connection()
        # 如果虚拟主机，并且bucket有变化，需要重新创建链接
        elif self.obsRequest.virtualHost:
            index = self.myHTTPConnection.host.index(self.myHTTPConnection.root_host)
            if index:  # 之前的连接有桶
                if not self.obsRequest.bucket:  # 之前的连接有桶,当前连接没有桶
                    self.myHTTPConnection.host = self.myHTTPConnection.root_host
                    self.myHTTPConnection.close_connection()
                    self.myHTTPConnection.create_connection()
                else:  # 之前的连接有桶,现在桶变化
                    previous_bucket = self.myHTTPConnection.host[0:index - 1]
                    if self.obsRequest.bucket != previous_bucket:  # 当前的桶与之前的连接桶变化
                        self.myHTTPConnection.host = self.obsRequest.bucket + '.' + self.myHTTPConnection.root_host
                        self.myHTTPConnection.close_connection()
                        self.myHTTPConnection.create_connection()
            elif self.obsRequest.bucket:  # 之前的连接没有桶,当前连接有桶
                self.myHTTPConnection.host = self.obsRequest.bucket + '.' + self.myHTTPConnection.root_host
                self.myHTTPConnection.close_connection()
                self.myHTTPConnection.create_connection()

        # 根据配置，添加connection
        if not self.myHTTPConnection.conn_header:
            if self.myHTTPConnection.longConnection:
                self.obsRequest.headers['Connection'] = 'keep-alive'
            else:
                self.obsRequest.headers['Connection'] = 'close'
        else:
            self.obsRequest.headers['Connection'] = self.myHTTPConnection.conn_header

    def _get_return_data_from_response_body_(self, body):
        if self.obsRequest.requestType not in ('ListObjectsInBucket', 'InitMultiUpload', 'CopyPart', 'CopyObject'):
            return None
        # 对于ListObjectsInBucket，返回marker
        if self.obsRequest.requestType == 'ListObjectsInBucket':
            if len(body) < 50:
                return None
            marker = re.findall('<NextMarker>.*</NextMarker>', body)
            if len(marker) > 0:
                marker = marker[0][12:-13].strip()
                if len(marker) > 0:
                    logging.debug('find next marker here %s' % marker)
                    return marker
        elif self.obsRequest.requestType == 'InitMultiUpload':
            upload_id = re.findall('<UploadId>.*</UploadId>', body)
            if len(upload_id) > 0:
                upload_id = upload_id[0][10:-11].strip()
                if len(upload_id) > 0:
                    logging.debug('find upload_id here %s' % upload_id)
                    return upload_id
        elif self.obsRequest.requestType == 'CopyPart' or self.obsRequest.requestType == 'CopyObject':
            etag = re.findall('<ETag>.*</ETag>', body)
            if len(etag) > 0:
                etag = etag[0][6:-7].strip()
                if len(etag) > 0:
                    logging.debug('find etag here %s' % etag)
                    return etag
        logging.info('find none in body %r' % body)
        return None

    @staticmethod
    def _get_request_id_from_body_(recv_body):
        if len(recv_body) < 50:
            return ''
        request_id = re.findall('<RequestId>.*</RequestId>', recv_body)
        if len(request_id) > 0:
            request_id = request_id[0][11:-12].strip()
            if len(request_id) > 0:
                logging.debug('find request here %s' % request_id)
                return request_id
        return '9999999999999997'

    @staticmethod
    def _get_header(response, header, is_http2=False, is_lower=False):
        """
        
        :param response: 
        :param header: 
        :param is_http2: 
        :param is_lower: 
        :return: 
        """
        result = None
        if is_http2:
            result = response.headers.get(header, None)
        else:
            result = response.getheader(header, None)

        return result.lower() if is_lower else result

    # cal_md5:计算请求request还是响应response的MD5, 默认为空，表示不计算。
    # 若计算请求的MD5，则在对象最后33个字节记录之前数据内容的!MD5,便于校验数据。
    # 同时需要保证工具不会上传带!内容的字符。
    def make_request(self, cal_md5=None, memory_file=None):
        # 如果计算MD5则随机一个CHUNK_SIZE,否则固定CHUNK_SIZE大小。
        if cal_md5:
            md5_hash_part = 0
            md5_hash_total = 0
            file_hash = hashlib.md5()
            check_data = False
            chunk_size = random.randint(4096, 1048576)
            logging.debug('chunk_size: %d' % chunk_size)
        else:
            chunk_size = 65536
        peer_addr = self.myHTTPConnection.host
        local_addr = ''
        http_response = None
        recv_body = ''
        self.defineResponse.start_time = time.time()
        try:
            stream_id = None
            if self.myHTTPConnection.is_http2:
                if self.myHTTPConnection.isSecure:
                    stream_id = self.myHTTPConnection.connection.putrequest(self.obsRequest.method, self.obsRequest.url,
                                                                            secure=True)
                else:
                    stream_id = self.myHTTPConnection.connection.putrequest(self.obsRequest.method, self.obsRequest.url)
                    logging.debug("streamId: %s" % str(stream_id))
            else:
                self.myHTTPConnection.connection.putrequest(self.obsRequest.method, self.obsRequest.url, skip_host=1)
            # 发送HTTP头域
            for k in self.obsRequest.headers.keys():
                if isinstance(self.obsRequest.headers[k], list):
                    for i in self.obsRequest.headers[k]:
                        if self.myHTTPConnection.is_http2:
                            self.myHTTPConnection.connection.putheader(header=str(k), argument=str(i), stream_id=stream_id)
                        else:
                            self.myHTTPConnection.connection.putheader(k, i)
                else:
                    if self.myHTTPConnection.is_http2:
                        self.myHTTPConnection.connection.putheader(header=str(k), argument=str(self.obsRequest.headers[k]), stream_id=stream_id)
                    else:
                        self.myHTTPConnection.connection.putheader(k, self.obsRequest.headers[k])
            if not self.myHTTPConnection.is_http2:
                self.myHTTPConnection.connection.endheaders()
                local_addr = str(self.myHTTPConnection.connection.sock._sock.getsockname())
                peer_addr = str(self.myHTTPConnection.connection.sock._sock.getpeername())
                logging.debug('Request:[%s], conn:[%s->%s], sendURL:[%s], sendHeaders:[%r], sendContent:[%s]' % (
                    self.obsRequest.requestType, local_addr, peer_addr, self.obsRequest.url, self.obsRequest.headers,
                    self.obsRequest.sendContent[0:1024]))
            else:
                if self.obsRequest.contentLength > 0 and not self.obsRequest.sendContent:
                    self.myHTTPConnection.connection.endheaders(final=False, stream_id=stream_id)
                elif not self.obsRequest.sendContent:
                    self.myHTTPConnection.connection.endheaders(final=True, stream_id=stream_id)
                else:
                    self.myHTTPConnection.connection.endheaders(final=False, stream_id=stream_id)
                local_addr = str(self.myHTTPConnection.connection._sock.getsockname())
                peer_addr = str(self.myHTTPConnection.connection._sock.getpeername())
                logging.debug('Request:[%s], conn:[%s->%s], sendURL:[%s], sendHeaders:[%r], sendContent:[%s]' % (
                    self.obsRequest.requestType, local_addr, peer_addr, self.obsRequest.url,
                    self.obsRequest.headers,
                    self.obsRequest.sendContent[0:1024]))
            # 发送body.如果self.obsRequest内的contentLength >0但content内容为空，则需要从内存构造。否则发送content
            if self.obsRequest.contentLength > 0 and not self.obsRequest.sendContent:
                if self.obsRequest.is_data_from_file:
                    with open(self.obsRequest.local_file_path, 'rb') as obj_to_put:
                        while self.defineResponse.send_bytes < self.obsRequest.contentLength:
                            if self.obsRequest.contentLength - self.defineResponse.send_bytes >= chunk_size:
                                bytestmp = obj_to_put.read(chunk_size)
                                self.defineResponse.send_bytes += chunk_size
                            else:
                                bytestmp = obj_to_put.read(self.obsRequest.contentLength - self.defineResponse.send_bytes)
                                self.defineResponse.send_bytes += (self.obsRequest.contentLength - self.defineResponse.send_bytes)
                            self.myHTTPConnection.connection.send(bytestmp)
                else:
                    # 发送数据。为避免影响发送的性能，对未打开MD5校验开关的请求，发送固定的字符。
                    if not cal_md5:
                        # 每个对象填充不同的随机字符，避开字符'!'(33)
                        # fill_char = chr(random.randint(34, 127))
                        # while self.defineResponse.send_bytes < self.obsRequest.contentLength:
                        #     if self.obsRequest.contentLength - self.defineResponse.send_bytes >= chunk_size:
                        #         bytestmp = fill_char * chunk_size
                        #     else:
                        #         bytestmp = fill_char * (self.obsRequest.contentLength - self.defineResponse.send_bytes)
                        #     self.myHTTPConnection.connection.send(bytestmp)
                        #     self.defineResponse.send_bytes += len(bytestmp)
                        # 从内容中读取不同内容的字符串
                        while self.defineResponse.send_bytes < self.obsRequest.contentLength:
                            pos = random.randint(0, 4096)
                            if self.obsRequest.contentLength - self.defineResponse.send_bytes >= chunk_size:
                                bytestmp = memory_file.read(chunk_size)
                                memory_file.seek(pos, 0)
                            else:
                                bytestmp = memory_file.read(self.obsRequest.contentLength - self.defineResponse.send_bytes)
                                memory_file.seek(pos, 0)
                            if self.myHTTPConnection.is_http2:
                                tmp = self.defineResponse.send_bytes + len(bytestmp)
                                if tmp >= self.obsRequest.contentLength:
                                    self.myHTTPConnection.connection.send(bytestmp, final=True, stream_id=stream_id)
                                else:
                                    self.myHTTPConnection.connection.send(bytestmp, stream_id=stream_id)
                            else:
                                self.myHTTPConnection.connection.send(bytestmp)
                            self.defineResponse.send_bytes += len(bytestmp)
                    else:  # cal_md5 == True
                        # 若打开cal_md5，预留33个字符在最后写MD5
                        data_size_to_done = self.obsRequest.contentLength - 33
                        # 将桶、对象、时间戳写入对象内容
                        fill_chars = '%s;%f;' % (self.obsRequest.recordUrl, self.defineResponse.start_time)
                        if '!' in fill_chars:
                            fill_chars = fill_chars.replace('!', ';')  # 将fill_chars中的'!'替换成';',防止MD5检测时冲突。
                        while self.defineResponse.send_bytes < data_size_to_done:
                            if data_size_to_done - self.defineResponse.send_bytes >= chunk_size:
                                bytestmp = chunk_size / len(fill_chars) * fill_chars + fill_chars[0:chunk_size % len(fill_chars)]
                            else:
                                datalen = data_size_to_done - self.defineResponse.send_bytes
                                bytestmp = datalen / len(fill_chars) * fill_chars + fill_chars[0:datalen % len(fill_chars)]
                            if self.myHTTPConnection.is_http2:
                                # tmp = self.defineResponse.send_bytes + len(bytestmp)
                                # if tmp >= self.obsRequest.contentLength:
                                #     if tmp >= self.obsRequest.contentLength:
                                #         self.myHTTPConnection.connection.send(bytestmp, final=True, stream_id=stream_id)
                                #     else:
                                self.myHTTPConnection.connection.send(bytestmp, stream_id=stream_id)
                            else:
                                self.myHTTPConnection.connection.send(bytestmp)
                            self.defineResponse.send_bytes += len(bytestmp)
                            file_hash.update(bytestmp)  # 逐个数据分片计算MD5
                        # 写MD5内容,之前已预留了33个字符位置
                        md5_hash_str = '!' + file_hash.hexdigest().zfill(32)
                        data_to_send = md5_hash_str[0:self.obsRequest.contentLength - self.defineResponse.send_bytes]  # 长度最大33
                        if self.myHTTPConnection.is_http2:
                            self.myHTTPConnection.connection.send(data_to_send, final=True, stream_id=stream_id)
                        else:
                            self.myHTTPConnection.connection.send(data_to_send)
                        self.defineResponse.send_bytes += len(data_to_send)
                        file_hash.update(data_to_send)
                        logging.debug('write MD5 [%s] to object done' % data_to_send)
                        md5_hash_total = file_hash.hexdigest()
            else:
                if self.myHTTPConnection.is_http2:
                    self.myHTTPConnection.connection.send(self.obsRequest.sendContent, final=True, stream_id=stream_id)
                else:
                    self.myHTTPConnection.connection.send(self.obsRequest.sendContent)
                self.defineResponse.send_bytes += len(self.obsRequest.sendContent)
                if cal_md5:
                    md5_hash_total = file_hash.hexdigest()

            wait_response_time_start = time.time()
            logging.debug('total send bytes: %d, content-length: %d' % (self.defineResponse.send_bytes, self.obsRequest.contentLength))
            # 接收响应
            if self.myHTTPConnection.is_http2:
                http_response = self.myHTTPConnection.connection.getresponse(stream_id=stream_id)
            else:
                http_response = self.myHTTPConnection.connection.getresponse(buffering=True)
            wait_response_time = time.time() - wait_response_time_start
            logging.debug('get response, wait time %.3f' % wait_response_time)
            # 读取响应体
            if self.myHTTPConnection.is_http2:
                content_length = http_response.headers.get(b'Content-Length', '-1')
                content_length = content_length[0] if isinstance(content_length, list) else content_length
                logging.debug('get ContentLength: %s' % content_length)
                self.defineResponse.request_id = http_response.headers.get(b'x-amz-request-id', '9999999999999998')[0]
                self.defineResponse.id2 = http_response.headers.get(b'x-amz-id-2', 'None')[0]
                self.defineResponse.position = http_response.headers.get(b'x-obs-next-append-position', "-1")
            else:
                content_length = int(http_response.getheader('Content-Length', '-1'))
                logging.debug('get ContentLength: %d' % content_length)
                self.defineResponse.request_id = http_response.getheader('x-amz-request-id', '9999999999999998')
                self.defineResponse.id2 = http_response.getheader('x-amz-id-2', 'None')
                self.defineResponse.position = http_response.getheader('x-obs-next-append-position', "-1")
            # 区分不同的请求，对于成功响应的GetObject请求，需要特殊处理,否则一次读完body内容。
            # 需要考虑range下载，返回2xx均为正常请求。
            if http_response.status < 300 and self.obsRequest.requestType == 'GetObject':
                # 同时满足条件，才校验数据内容。
                # 1.打开cal_md5开关。2.GetObject操作；3.正确返回200响应(206不计算）；4.对象元数据包含x-amz-meta-md5written为认为是本工具上传的对象
                if cal_md5:
                    md5_in_obj = ''
                    last_datatmp = ''
                    if http_response.status == 200 and self._get_header(http_response, 'x-amz-meta-md5written', self.myHTTPConnection.is_http2):
                        check_data = True
                        logging.info("check data content open")
                while True:
                    datatmp = http_response.read(chunk_size)
                    if not datatmp:
                        logging.info('datatmp is empty, break cycle')
                        recv_body = '[receive content], length: %d' % self.defineResponse.recv_bytes
                        break
                    self.defineResponse.recv_bytes += len(datatmp)
                    if cal_md5:
                        last_datatmp = datatmp
                        if '!' in datatmp:
                            md5_start = datatmp.find('!')
                            logging.debug('find ! in datatmp index: %d, datatmp len: %d' % (md5_start, len(datatmp)))
                            md5_in_obj = datatmp[md5_start:]
                            logging.debug('part MD5InObj: [%s]' % md5_in_obj)
                            file_hash.update(datatmp[0:md5_start])
                            md5_hash_part = file_hash.hexdigest()
                            logging.debug('calculate part MD5 [%r] for get operation' % md5_hash_part)
                            file_hash.update(md5_in_obj)
                        else:
                            file_hash.update(datatmp)
                            if 0 < len(md5_in_obj) < 33:
                                md5_in_obj += datatmp[:33 - len(md5_in_obj)]
                                logging.debug('finally MD5InObj: [%s]' % md5_in_obj)
            else:
                recv_body = http_response.read()
                self.defineResponse.recv_bytes = len(recv_body)
            # 要读完数据才算请求结束
            self.defineResponse.end_time = time.time()
            self.defineResponse.status = str(http_response.status) + ' ' + http_response.reason
            # 记日志、重定向(<400:debug; >=400,<500: warn; >=500:error)
            http_response.status = int(http_response.status)
            if http_response.status < 400:
                logging.debug(
                    'Request:[%s], conn: [%s->%s], URL:[%s], wait_response_time:[%.3f], responseStatus:[%s], %r, %r' % (
                        self.obsRequest.requestType, local_addr, peer_addr, self.obsRequest.url, wait_response_time,
                        self.defineResponse.status, str(http_response.msg) if not self.myHTTPConnection.is_http2 else "", recv_body[0:1024]))
                if http_response.status in [300, 301, 302, 303, 307]:
                    # 从Location获取地址,重新请求
                    if not self._get_header(http_response, 'location', self.myHTTPConnection.is_http2):
                        logging.error('request return 3xx without header location')
                    else:
                        urlobj = urlparse(self._get_header(http_response, 'location', self.myHTTPConnection.is_http2))
                        if not urlobj.scheme or not urlobj.hostname:
                            logging.error('location format error [%s] ' % self._get_header(http_response, 'location', self.myHTTPConnection.is_http2))
                        else:
                            logging.debug('redirect hostname: %s, url:%s' % (urlobj.hostname, urlobj.path))
                            # 关闭本次连接，重新初始化新连接。
                            self.myHTTPConnection.close_connection()
                            # 深拷贝1个临时连接， 按重定向要求修改。
                            self.myCopyHTTPConnection = copy.deepcopy(self.myHTTPConnection)
                            self.myCopyHTTPConnection.isSecure = (urlobj.scheme == 'https')
                            self.myCopyHTTPConnection.host = urlobj.hostname
                            # 互换2个连接对象,请求后换回，后续请求继续使用。
                            self.myCopyHTTPConnection, self.myHTTPConnection = self.myHTTPConnection, self.myCopyHTTPConnection
                            # 更新url和host重新请求
                            self.obsRequest.url = urlobj.path
                            self.obsRequest.add_host_header(urlobj.hostname)
                            self.__init__(self.obsRequest, self.myHTTPConnection)
                            logging.info(
                                'redirect the request to %s%s' % (self.myHTTPConnection.host, self.obsRequest.url))
                            self.make_request(cal_md5, memory_file)
                            return
            elif http_response.status < 500:
                logging.warn(
                    'Request:[%s], conn: [%s->%s], URL:[%s], wait_response_time:[%.3f], responseStatus:[%s], %r, %r' % (
                        self.obsRequest.requestType, local_addr, peer_addr, self.obsRequest.url, wait_response_time,
                        self.defineResponse.status, str(http_response.msg) if not self.myHTTPConnection.is_http2 else "", recv_body[0:1024]))
            else:
                logging.error(
                    'Request:[%s], conn: [%s->%s], URL:[%s], wait_response_time: [%.3f], responseStatus:[%s], %r, %r' % (
                        self.obsRequest.requestType, local_addr, peer_addr, self.obsRequest.url, wait_response_time,
                        self.defineResponse.status,
                        str(http_response.msg) if not self.myHTTPConnection.is_http2 else "", recv_body[0:1024]))
                if http_response.status == 503:
                    flow_controll_msg = 'Service unavailable, local data center is busy'
                    if recv_body.find(flow_controll_msg) != -1:
                        self.defineResponse.status = '503 Flow Control'  # 标记外部流控
            if self.obsRequest.requestType == 'PutObject':
                self.defineResponse.return_data = self._get_header(http_response, 'x-amz-version-id', self.myHTTPConnection.is_http2)
                logging.debug('get x-amz-version-id: %s' % self.defineResponse.return_data)
            elif self.obsRequest.requestType == 'UploadPart':
                self.defineResponse.return_data = self._get_header(http_response, 'Etag', self.myHTTPConnection.is_http2)
            else:
                self.defineResponse.return_data = self._get_return_data_from_response_body_(recv_body)

            # 部分错误结果的头域中没有包含x-amz-request-id,则从recv_body中获取
            if self.defineResponse.request_id == '9999999999999998' and http_response.status >= 300:
                self.defineResponse.request_id = self._get_request_id_from_body_(recv_body)
            # 在MD5校验前先检查一次数据长度
            if self.obsRequest.method != 'HEAD' and str(content_length) != str(-1) and str(content_length) != str(self.defineResponse.recv_bytes):
                logging.error(
                    'data error. content_length %d != recvBytes %d' % (int(content_length), self.defineResponse.recv_bytes))
                raise Exception("Data Error Content-Length")
            # 长度正确，校验MD5值
            if cal_md5 and http_response.status < 300 and self.obsRequest.requestType == 'GetObject':
                md5_hash_total = file_hash.hexdigest()
                if check_data:
                    # 没有找到MD5值
                    if not md5_in_obj and self.defineResponse.recv_bytes:
                        logging.error(
                            'data error. can not find MD5 written in object, object size: %d' % self.defineResponse.recv_bytes)
                        raise Exception("Data Error MD5")
                    # 找到MD5值，不匹配。
                    elif md5_in_obj and md5_in_obj[1:] not in md5_hash_part:
                        logging.error('data error. MD5 [%s] of data loaded != MD5 [%s] recorded in object %s/%s' % (
                            md5_hash_part, md5_in_obj, self.obsRequest.bucket, self.obsRequest.key))
                        logging.error('last_datatmp:[%r]' % last_datatmp)
                        raise Exception("Data Error MD5")
                    elif md5_in_obj and md5_in_obj[1:] in md5_hash_part:
                        logging.info('check object data MD5 OK, MD5 in object [%s], object size: %d' % (
                            md5_in_obj, self.defineResponse.recv_bytes))
        except KeyboardInterrupt:
            if not self.defineResponse.status:
                self.defineResponse.status = '9991 KeyboardInterrupt'
        except Exception, data:
            import traceback
            stack = traceback.format_exc()
            logging.error(
                'Caught exception:%s, Request:[%s], conn: [local:%s->peer:%s], URL:[%s], responseStatus:[%s], responseBody:[%r]'
                % (
                    data, self.obsRequest.requestType, local_addr, peer_addr, self.obsRequest.url,
                    self.defineResponse.status,
                    recv_body[0:1024]))
            logging.error('print stack: %s' % stack)
            self.defineResponse.status = self._get_http_status_from_exception_(data, stack)
            logging.debug('self.defineResponse.status %s from except' % self.defineResponse.status)
        finally:
            # 互换2个对象
            if self.myCopyHTTPConnection:
                self.myCopyHTTPConnection, self.myHTTPConnection = self.myHTTPConnection, self.myCopyHTTPConnection

            if self.defineResponse.end_time == 0.0:
                self.defineResponse.end_time = time.time()

            if cal_md5:
                self.defineResponse.content_md5 = md5_hash_total
            # 关闭连接：1.按服务端语义，若connection:close，则关闭连接。
            if self.myHTTPConnection.is_http2:
                if http_response and ('close' == http_response.headers.get(b'connection', '').lower()
                                      or 'close' == http_response.headers.get(b'Connection', '').lower()):
                    # 关闭连接，让后续请求再新建连接。
                    logging.info('server inform to close connection')
                    self.myHTTPConnection.close_connection()
            else:
                if http_response and ('close' == http_response.getheader('connection', '').lower()
                                      or 'close' == http_response.getheader('Connection', '').lower()):
                    # 关闭连接，让后续请求再新建连接。
                    logging.info('server inform to close connection')
                    self.myHTTPConnection.close_connection()
                # 2.客户端感知的连接类错误，关闭连接。
                elif self.defineResponse.status > '9910':
                    # logging.warning('request error %s, close and reconnect' %self.defineResponse.status)
                    # 很可能是网络异常，关闭连接，让后续请求再新建连接。
                    self.myHTTPConnection.close_connection()
                    time.sleep(.1)
                # 3.客户端配置了短连接
                elif not self.myHTTPConnection.longConnection:
                    self.myHTTPConnection.close_connection()
                    # python 2.7以下存在bug，不能直接使用close()方法关闭连接，不然客户端存在CLOSE_WAIT状态。
                    if self.myHTTPConnection.isSecure:
                        try:
                            import sys

                            if sys.version < '2.7':
                                import gc

                                gc.collect(0)
                        except Exception, e:
                            logging.warning('make gc exception: %s' % e)
            logging.debug('finally result: %s' % self.defineResponse.to_string)
            return self.defineResponse

    @staticmethod
    def _get_http_status_from_exception_(data, stack):
        error_map = {
            'connection reset by peer': '9998',  # 连接类错误：服务器拒绝连接
            'broken pipe': '9997',  # 读写过程中连接管道破裂
            'timed out': '9996',  # 客户端等服务器端响应时间超时，时间配置参数ConnectTimeout
            'badstatusline': '9995',  # 客户端读HTTP响应码格式错误或读到为空，常见于服务器端断开连接
            'connection timed out': '9994',  # 请求前连接建立超时
            'the read operation timed out': '9993',  # 从服务器端读响应超时
            'cannotsendrequest': '9992',  # 客户端发送请求报错
            'keyboardinterrupt': '9991',  # 键盘Ctrl+C中断请求
            'name or service not known': '9990',  # 服务器端域名无法解析
            'no route to host': '9989',  # 到服务器端IP不可达，路由错误
            'data error md5': '9901',  # 下载对象数据校验错误，也可能数据长度不正确
            'data error content-length': '9902',  # 收到消息长度与服务器端返回的content-length头域值不一致
            'other error': '9999'  # 其它错误，参考工具日志堆栈定位。直接搜索堆栈关键字。
        }
        data = str(data).strip()
        if not data and stack:
            stack = stack.strip()
            data = stack[stack.rfind('\n') + 1:]
        if not data:
            data = 'other error'
        for (key, value) in error_map.items():
            if key in data.lower():
                return '%s %s' % (value, data)
        return '9999 %s' % data
