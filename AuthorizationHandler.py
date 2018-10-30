# -*- coding:utf-8 -*-

"""
Handles authentication 
"""
import hashlib
import base64
import urllib
import Util
import time
import logging
import hmac
import datetime

try:
    from hashlib import sha256 as sha256
except ImportError:
    sha256 = None
    logging.error('import sha256 error')


class HmacAuthV2Handler:
    """
    Implements the Version 2 HMAC authorization.
    """

    def __init__(self, obsRequest):
        self.obsRequest = obsRequest

    def handle(self):
        self.obsRequest.headers['x-amz-date'] = time.strftime(Util.TIME_FORMAT, time.gmtime())

        if self.obsRequest.ak == '' or self.obsRequest.sk == '':
            logging.debug('ak [%s], sk [%s], return' % (self.obsRequest.ak, self.obsRequest.sk))
            return
        self.obsRequest.headers['Authorization'] = "AWS %s:%s" % (self.obsRequest.ak, self.encode(self.obsRequest.sk, self.__canonical_string__()))

        logging.debug('Authorization: [%s]' % (self.obsRequest.headers['Authorization']))

        if self.obsRequest.is_cdn:
            if self.obsRequest.cdn_ak == '' or self.obsRequest.cdn_sk == '':
                logging.debug('cdn ak [%s], cdn sk [%s], return' % (self.obsRequest.cdn_ak, self.obsRequest.cdn_sk))
                return
            self.obsRequest.headers['CloudService'] = "AWS %s:%s" % (self.obsRequest.cdn_ak, self.encode(self.obsRequest.cdn_sk, self.__canonical_string__()))
            self.obsRequest.headesr['CloudServiceSecurityToken'] = self.obsRequest.cdn_sts_token

            logging.debug('Cdn CloudService: [%s]' % (self.obsRequest.headers['CloudService']))
            logging.debug('Cdn CloudServiceSecurityToken: [%s]' % (self.obsRequest.headers['CloudServiceSecurityToken']))

    def encode(self, aws_secret_access_key, encodestr, urlencode=False):
        b64_hmac = base64.encodestring(hmac.new(aws_secret_access_key, encodestr, hashlib.sha1).digest()).strip()
        if urlencode:
            return urllib.quote_plus(b64_hmac)
        else:
            return b64_hmac

    def __canonical_string__(self):
        interesting_headers = {}
        for header_key in self.obsRequest.headers:
            lk = header_key.lower()
            if lk in ['content-md5', 'content-type', 'date'] or lk.startswith('x-amz-'):
                interesting_headers[lk] = self.obsRequest.headers[header_key].strip()
        # these keys get empty strings if they don't exist
        if not interesting_headers.has_key('content-type'):
            interesting_headers['content-type'] = ''
        if not interesting_headers.has_key('content-md5'):
            interesting_headers['content-md5'] = ''
        # just in case someone used this.  it's not necessary in this lib.
        if interesting_headers.has_key('x-amz-date'):
            interesting_headers['date'] = ''
        sorted_header_keys = interesting_headers.keys()
        sorted_header_keys.sort()
        c_string = "%s\n" % self.obsRequest.method
        for header_key in sorted_header_keys:
            if header_key.startswith('x-amz-'):
                c_string += "%s:%s\n" % (header_key, interesting_headers[header_key])
            else:
                c_string += "%s\n" % interesting_headers[header_key]

        # 根据virtualHost，桶，对象生成计算签名的c_string
        if self.obsRequest.bucket:
            c_string += "/%s" % urllib.quote_plus(self.obsRequest.bucket)
        c_string += "/%s" % urllib.quote_plus(self.obsRequest.key)

        if not self.obsRequest.queryArgs:
            logging.debug('StrToSign: [%r]' % c_string)
            return c_string

        # 添加queryArgs到c_string, value不需要编码
        interesting_querys = (
        'acl', 'lifecycle', 'location', 'logging', 'notification', 'partNumber', 'policy', 'requestPayment',
        'versioning', 'torrent', 'uploadId', 'uploads', 'versionId', 'versioning', 'versions', 'website', 'delete',
        'deletebucket', 'cors', 'restore', 'append', 'position', 'x-image-process')
        c_string += '?'
        for arg in sorted(self.obsRequest.queryArgs):
            if arg not in interesting_querys:
                continue
            if c_string[-1:] != '?':
                c_string += '&%s' % arg
            else:
                c_string += '%s' % arg
            if self.obsRequest.queryArgs[arg]:
                c_string += '=%s' % (self.obsRequest.queryArgs[arg])
        if c_string[-1:] == '?':
            c_string = c_string[:-1]
        logging.debug('StrToSign: [%r]' % c_string)
        return c_string


class HmacAuthV4Handler:
    """
    Implements the Version 4 HMAC authorization.
    """

    def __init__(self, obsRequest, service_name='s3'):
        # You can set the service_name and region_name to override the values
        self.service_name = service_name
        self.obsRequest = obsRequest

    def handle(self):
        now = datetime.datetime.utcnow()
        self.obsRequest.headers['x-amz-date'] = now.strftime('%Y%m%dT%H%M%SZ')

        unsignPayload = 'UNSIGNED-PAYLOAD'
        self.obsRequest.headers['x-amz-content-sha256'] = unsignPayload

        canonical_request = self.canonical_request()
        logging.debug('CanonicalRequest: [%r]' % canonical_request)
        string_to_sign = self.string_to_sign(canonical_request)
        logging.debug('StrToSign:[%r]' % string_to_sign)
        signature = self.signature(string_to_sign)
        logging.debug('Signature: [%r]' % signature)
        headers_to_sign = self.headers_to_sign()
        l = ['AWS4-HMAC-SHA256 Credential=%s' % self.getScope(withAK=True)]
        l.append('SignedHeaders=%s' % self.signed_headers(headers_to_sign))
        l.append('Signature=%s' % signature)
        self.obsRequest.headers['Authorization'] = ','.join(l)
        logging.debug('Authorization: [%r]' % (self.obsRequest.headers['Authorization']))

    def signature(self, string_to_sign):
        s_key = self.obsRequest.sk
        k_date = self._sign(('AWS4' + s_key).encode('utf-8'),
                            self.obsRequest.headers['x-amz-date'][0:8])
        k_region = self._sign(k_date, self.obsRequest.region)
        k_service = self._sign(k_region, self.service_name)
        k_signing = self._sign(k_service, 'aws4_request')
        return self._sign(k_signing, string_to_sign, hex=True)

    def _sign(self, key, msg, hex=False):
        if not isinstance(key, bytes):
            key = key.encode('utf-8')
        if hex:
            sig = hmac.new(key, msg.encode('utf-8'), sha256).hexdigest()
        else:
            sig = hmac.new(key, msg.encode('utf-8'), sha256).digest()
        return sig

    def headers_to_sign(self):
        """
        Select the headers from the obsRequest that need to be included in the StringToSign.
        And make sure the headsers are in lower case
        """
        headersToSign = {'host': self.obsRequest.headers['Host']}
        for name, value in self.obsRequest.headers.items():
            if not name or not value: continue
            lname = name.lower().strip()
            if lname.startswith('x-amz') or lname == 'content-type':
                if isinstance(value, bytes): value = value.decode('utf-8')
                headersToSign[lname] = value
        return headersToSign

    def canonical_uri(self):
        if '?' in self.obsRequest.url:
            return urllib.quote_plus(self.obsRequest.url[0:self.obsRequest.url.find('?')], safe='/&?%')
        else:
            return urllib.quote_plus(self.obsRequest.url, safe='/&?%')

    def query_string(self):
        parameterNames = sorted(self.obsRequest.queryArgs.keys())
        pairs = []
        for pname in parameterNames:
            pval = Util.get_utf8_value(self.obsRequest.getQuerysArgs()[pname])
            pairs.append(urllib.quote_plus(pname, safe='') + '=' + urllib.quote_plus(pval, safe='-_~'))
        return '&'.join(pairs)

    def canonical_query_string(self):
        # POST requests pass parameters in through the
        # http_request.body field.
        # if self.obsRequest.method == 'POST':
        #    return ""
        l = []
        for param in sorted(self.obsRequest.queryArgs):
            value = Util.get_utf8_value(self.obsRequest.queryArgs[param])
            l.append('%s=%s' % (urllib.quote_plus(param, safe='-_.~'),
                                urllib.quote_plus(value, safe='-_.~')))
        logging.debug('query_string: ' + '&'.join(l))
        return '&'.join(l)

    def canonical_headers(self, headersToSign):
        # 已经都是小写，直接排序
        return '\n'.join(['%s:%s' % (key, headersToSign[key]) for key in sorted(headersToSign.keys())])

    def signed_headers(self, headersToSign):
        return ';'.join(sorted(['%s' % n for n in headersToSign]))

    def payload(self):
        # use unsigned payload option.
        return 'UNSIGNED-PAYLOAD'

    def canonical_request(self):
        payload = self.payload()
        cr = [self.obsRequest.method]
        cr.append(self.canonical_uri())
        cr.append(self.canonical_query_string())
        headersToSign = self.headers_to_sign()
        cr.append(self.canonical_headers(headersToSign) + '\n')
        cr.append(self.signed_headers(headersToSign))
        cr.append(payload)
        return '\n'.join(cr)

    def getScope(self, withAK=False):
        timeStamp = self.obsRequest.headers['x-amz-date'][0:8]
        aws4Request = 'aws4_request'
        if withAK:
            return '%s/%s/%s/%s/%s' % (
            self.obsRequest.ak, timeStamp, self.obsRequest.region, self.service_name, aws4Request)
        else:
            return '%s/%s/%s/%s' % (timeStamp, self.obsRequest.region, self.service_name, aws4Request)

    def string_to_sign(self, canonicalRequest):
        timeStamp = self.obsRequest.headers['x-amz-date']
        canonicalRequest = sha256(canonicalRequest.encode('utf-8')).hexdigest()
        return 'AWS4-HMAC-SHA256\n%s\n%s\n%s' % (timeStamp, self.getScope(), canonicalRequest)
