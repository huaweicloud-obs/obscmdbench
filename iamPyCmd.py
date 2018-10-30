#!/usr/bin/python
# -*- coding:utf-8 -*- 
import os
import base64
import hmac,hashlib
import httplib,urllib
import logging
import time
import socket
import re
from datetime import datetime
try:
    import ssl
except ImportError:
    logging.warning('import ssl module error')

############################以下参数可以修改############################
#请求者的证书，可以是帐户管理员、帐户、用户
AWSAccessKeyId = 'B87509F0FE8224150C45'           
AWSSecretKey = 'h4yzhMkKHVAMqUsDPbuV1M6W4bMAAAFE/oIkF9'
# POE服务地址，格式: IP:port
Host = '129.7.159.253:8083'
# 请求是否采用HTTPS方式，可选值 True | False
Https = False

# HTTPS 加密协议版本，可选TLSv1 | TLSv1_1 | TLSv1_2 | SSLv23 | SSLv2 | SSLv3 (当Https为True时有效)
SslVersion = 'SSLv23'

# 请求加密算法，可选值  HmacSHA1 | HmacSHA256
SignatureMethod = 'HmacSHA1'

# 请求签名算法版本
SignatureVersion = '2'

############################以上参数可以修改#############################

##用于请求的相关信息
Configs = {"AWSAccessKeyId":AWSAccessKeyId, "AWSSecretKey":AWSSecretKey,"Host":Host, "Https":Https, "SslVersion": SslVersion, "Url":"/poe/rest","Method":"GET"}
#用于URL中的参数
Params = {"AWSAccessKeyId":AWSAccessKeyId, "SignatureVersion":SignatureVersion, "SignatureMethod":SignatureMethod, "Signature":'',"Timestamp":'', "Expires":''}

Console_LogLevel = None
def initLocalLogging():
    global Console_LogLevel 
    global logging
    LogFile_LogLevel = logging.INFO 
    Console_LogLevel = logging.INFO
    LogFile = str(__file__) +'.log' #logfile path
    logging.basicConfig(level=LogFile_LogLevel,
                format='%(asctime)s %(filename)s[line:%(lineno)d] [%(thread)d] [%(levelname)s] %(message)s',
                filename=LogFile,
                filemode='w')
    logging.error('logging level: %s', logging.getLevelName(LogFile_LogLevel))  

#This part describe where and how the accounts stored.USER_FILE is the file where the account stored, and USER_FORMAT is the format of the account info stored in the file.
USER_FILE = 'users.dat'

TIME_FORMAT = '%a, %d %b %Y %H:%M:%S GMT'

#Action list                 ['操作类型', '必选的参数个数（必须的参数必须放在最前面）','参数1','参数2'...]
createAccountFields        = ['CreateAccount',        2, 'AccountId', 'AccountName', 'CanonicalUserId','Email']
createAccountWithAllFields = ['CreateAccountWithAll', 2, 'AccountId', 'AccountName', 'CanonicalUserId', 'Email']
updateAccountFields        = ['UpdateAccount',        1, 'AccountId', 'NewAccountName', 'NewEmail', 'Status']
getAccountFields           = ['GetAccount',           1, 'AccountId']
listAccountsFields         = ['ListAccounts',         0, 'Marker', 'MaxItems', 'StartIndex', 'EndIndex', 'QueryName', 'Sort', 'QueryCounts', 'SortByName']
getAccountStatisticFields  = ['GetAccountStatistic',  0, 'AccountId']
deleteAccountFields        = ['DeleteAccount',        1, 'AccountId']
getAccountSummaryFields    = ['GetAccountSummary',    0]
createAccessKeyFields      = ['CreateAccessKey',      0, 'AccountId', 'UserName']
updateAccessKeyFields      = ['UpdateAccessKey',      2, 'AccessKeyId', 'Status', 'AccountId' ,'UserName']
listAccessKeysFields       = ['ListAccessKeys',       0, 'AccountId', 'UserName']
deleteAccessKeyFields      = ['DeleteAccessKey',      1, 'AccessKeyId', 'AccountId', 'UserName']
importAccessKeyFields      = ['ImportAccessKey',      3, 'AccountId', 'AccessKeyId', 'SecretAccessKey']
createServiceFields        = ['CreateService',        2, 'AccountId','ServiceType']
updateServiceFields        = ['UpdateService',        3, 'AccountId','ServiceType', 'Status']
listServicesFields         = ['ListServices',         1, 'AccountId']
deleteServiceFields        = ['DeleteService',        2, 'AccountId','ServiceType']
putStoragePolicyFields     = ['PutStoragePolicy',     2, 'AccountId', 'PolicyDocument']
getStoragePolicyFields     = ['GetStoragePolicy',     1, 'AccountId']
getSummaryFields           = ['GetSummary',           0]
getDataCenterFields        = ['GetDataCenter',        1, 'DataCenterId']
createMDCPolicyFields      = ['CreateMDCPolicy',      2, 'PolicyName','PolicyDocument']
listMDCPoliciesFields      = ['ListMDCPolicies',      0]
updateMDCPolicyFields      = ['UpdateMDCPolicy',      1, 'PolicyName', 'NewPolicyName','PolicyDocument']
deleteMDCPolicyFields      = ['DeleteMDCPolicy',      1, 'PolicyName']
putAccountMDCPolicyFields  = ['PutAccountMDCPolicy',  1, 'AccountId','PolicyNameList','DefaultPolicyName']
getAccountMDCPolicyFields  = ['GetAccountMDCPolicy',  0, 'AccountId']
createGroupFields          = ['CreateGroup',          1, 'GroupName','Path']
getGroupFields             = ['GetGroup',             1, 'GroupName','Marker','MaxItems']
deleteGroupFields          = ['DeleteGroup',          1, 'GroupName']
updateGroupFields          = ['UpdateGroup',          1, 'GroupName','NewGroupName','NewPath']
listGroupsFields           = ['ListGroups',           0, 'Marker','MaxItems','PathPrefix']
createUserFields           = ['CreateUser',           1, 'UserName','Path']
getUserFields              = ['GetUser',              1, 'UserName']
deleteUserFields           = ['DeleteUser',           1, 'UserName']
updateUserFields           = ['UpdateUser',           1, 'UserName','NewUserName','NewPath']
listUsersFields            = ['ListUsers',            0, 'Marker','MaxItems','PathPrefix']
addUserToGroupFields       = ['AddUserToGroup',       2, 'GroupName','UserName']
removeUserFromGroupFields  = ['RemoveUserFromGroup',  2, 'GroupName','UserName']
listGroupsForUserFields    = ['ListGroupsForUser',    1, 'UserName','Marker','MaxItems']
putUserPolicyFields        = ['PutUserPolicy',        3, 'UserName','PolicyName','PolicyDocument']
deleteUserPolicyFields     = ['DeleteUserPolicy',     2, 'UserName','PolicyName']
getUserPolicyFields        = ['GetUserPolicy',        2, 'UserName','PolicyName']
listUserPoliciesFields     = ['ListUserPolicies',     1, 'UserName','Marker','MaxItems']
putGroupPolicyFields       = ['PutGroupPolicy',       3, 'GroupName','PolicyName','PolicyDocument']
deleteGroupPolicyFields    = ['DeleteGroupPolicy',    2, 'GroupName','PolicyName']
listGroupPoliciesFields    = ['ListGroupPolicies',    1, 'GroupName','Marker','MaxItems']
getGroupPolicyFields       = ['GetGroupPolicy',       2, 'GroupName','PolicyName']

class DefineResponse:
        def __init__(self,status = 9999,reason = '', requestID= '', start_time = time.time(), end_time = 0.0, marker= '', needRetry = False, needReconnect = False):
            self.status = status
            self.reason = reason
            self.requestID = requestID
            self.marker = marker
            self.start_time = start_time
            self.end_time = end_time
            self.needRetry = needRetry
            self.needReconnect = needReconnect
        def toString(self):
            return 'RequestID: [' + self.requestID  + '], status:[' + str(self.status) + '], reason:[' + self.reason + '], marker:[' + \
            str(self.marker) + '], start_time: [' + str(self.start_time) + '], end_time: [' + str(self.end_time) +']' + ', needRetry:][' + \
            str(self.needRetry) + '], needReconnect: [' + str(self.needReconnect) +']'

class IAMRest:
    def __init__(self, params={}, configs={}, conn = None):
        self.params = params
        self.configs = configs
        self.conn = conn
        #若用户未指定，使用系统默认配置。
        for (k,v) in Params.iteritems():
            if k != 'Signature' and k not in params.keys(): 
                self.params[k] = v
        for (k,v) in Configs.iteritems():
            if k not in configs.keys():
                self.configs[k] = v
        if self.params['Timestamp'] == '' and self.params['Expires'] == '':
            self.params['Timestamp'] = datetime.now().isoformat()
            del self.params['Expires']
        elif self.params['Timestamp'] == '':
            del self.params['Timestamp']
        elif self.params['Expires'] == '':
            del self.params['Expires']
    
    def operate(self):
        Action = self.params['Action']
        Action = Action[0].lower() + Action[1:]
        try:
            opArray = globals()[Action+'Fields']
        except Exception, f:
            logging.error('Action "' + self.params['Action'] +'" error or NOT supported')
            _print_usage_()
            sys.exit()
        request = Request(self._generateURL_(),self.configs, self.params,{}, '', self.conn)
        resp = request.make_request()
        if Console_LogLevel == logging.INFO: 
            print '\033[0;32;40mtime consumed: %d ms\033[0m'  % ((resp.end_time-resp.start_time)*1000)
        logging.info('%s, time consumed: %d ms' % (resp.toString(),  int((resp.end_time-resp.start_time)*1000)))
        return resp

    def _generateURL_(self):
        canonical_string,url_str = self._get_canonical_string_()
        url_str = self.configs['Url'] + '?' + url_str
        # 如果用户指定了Signature，则不计算签名。
        if self.params.has_key('Signature'):
            return url_str
        logging.info('SignatureMethod:[%s], SignatureVersion:[%s]' % (SignatureMethod, SignatureVersion))
        signature = self._encode_(canonical_string)
        logging.debug('canonical_string: [%s], signature:[%s]' % (canonical_string, signature)) 
        url_str += '&Signature=' + urllib.quote(signature, '')
        return url_str

#canonical_string: used for cal signature url_str: used for request url
    def _get_canonical_string_(self):
        method = str(self.configs['Method'])
        host =  str(self.configs['Host'])
        url =  str(self.configs['Url'])
        canonical_string = ''
        canonical_string += method + '\n'
        url_str = ''
        if host.split(':')[1] == '80' or host.split(':')[1] == '443':
            canonical_string += host.split(':')[0] + '\n'
        else:
            canonical_string += host + '\n'
        canonical_string += url + '\n'
        for k in sorted(self.params):
            url_str += k + '=' + urllib.quote(self.params[k], '') + "&"
        if url_str.endswith("&"):
            url_str = url_str[0:len(url_str)-1]
        canonical_string += url_str
        return canonical_string,url_str

# computes the base64'ed hmac-sha hash of the canonical string and the secret access key, optionally urlencoding the result
    def _encode_(self,canonical_string):
        if self.params['SignatureMethod'] == 'HmacSHA1':
            return base64.encodestring(hmac.new(self.configs['AWSSecretKey'], canonical_string, hashlib.sha1).digest()).strip()
        else:
            return base64.encodestring(hmac.new(self.configs['AWSSecretKey'], canonical_string, hashlib.sha256).digest()).strip()
    
class Request:
    def __init__(self, url='',configs={},params={}, headers={},data='', conn = None):
        self.is_secure = configs['Https'] 
        self.sslVersion = ssl.__dict__['PROTOCOL_' + configs['SslVersion']]
        self.server = configs['Host'].split(':')[0]
        self.port = configs['Host'].split(':')[1]
        self.method = configs['Method']
        self.url = url
        self.headers  = headers
        self.data = data
        self.params = params
        self.configs = configs
        self.defineResponse = DefineResponse(status = 9999,reason = '', requestID= '', start_time = time.time(), end_time = 0.0, marker= '', needRetry = False)
        self.connection = conn
        if self.connection:
            self.headers['Connection'] = 'Keep-alive'
            self.longConnection = True
        else:
            self.headers['Connection'] = 'close'
            self.longConnection = True
        
        if Console_LogLevel == logging.INFO:
            httplib.HTTPConnection.debuglevel = 1
            
    def _init_Connection_(self):
        if self.is_secure:
            try:
                self.connection = httplib.HTTPSConnection(self.server + ':' + self.port, timeout=80, context=ssl.SSLContext(self.sslVersion))
            except Exception, e:
                logging.warn("init https connection error, %s, please update python version to 2.7.9+" %e)
                self.connection = httplib.HTTPSConnection("%s:%s" % (self.server, self.port))
        else:               
            self.connection = httplib.HTTPConnection("%s:%s" % (self.server, self.port))

    def close_connnection(self):
        if self.connection:
            self.connection.close()
    def open_connnection(self):
        if self.connection:
            self.connection.connect()
        
    def recordAKSK(self, userMsg):
        if USER_FILE == '': return
        ak = ''; sk = ''; id = '';
        for keyword in ['AccountId', 'UserName']:
            id = re.findall('<%s>.*</%s>' %(keyword,keyword), userMsg)
            if len(id) > 0: 
                id = id[0][len(keyword)+2:-len(keyword)-3]
                break
        ak = re.findall('<AccessKeyId.*AccessKeyId>',userMsg)
        if len(ak) >0: ak = ak[0][13:-14]
        sk = re.findall('<SecretAccessKey.*SecretAccessKey>',userMsg)
        if len(sk) >0: sk = sk[0][17:-18]
        if not id or not ak or not sk: 
            logging.warn('id:[%s],ak:[%s],sk[%s] invalid')
            return
        userFile = open(USER_FILE, 'a')
        userFile.writelines('%s,%s,%s,\n' %(id,ak,sk))
        userFile.close()       
    
    def _needGetMarker_(self):
        if self.defineResponse.status != 200: return False
        if not self.params.has_key('Action'): return False
        if self.params['Action'] != 'ListAccounts' and self.params['Action'] != 'ListUsers' and \
        self.params['Action'] != 'ListGroupsForUser' and self.params['Action'] != 'GetGroup' and \
        self.params['Action'] != 'ListGroups' and self.params['Action'] != 'ListGroupPolicies' and\
        self.params['Action'] != 'ListUserPolicies': return False
        return True
                    
    def _getReqeustidFromResponse_(self, body):
        #to be done
        import re
        requestId = re.findall('<RequestId>.*</RequestId>', body)
        if len(requestId) > 0:
            return requestId[0][11:-12]
        else:
            return ''
    
    def _getMarkerFromResponseBody_(self, body):
        if len(body) < 50:
            return None
        import re
        marker = re.findall('<Marker>.*</Marker>', body)
        if len(marker) > 0:
            logging.debug('find marker here %s' % marker)
            marker = marker[0][8:-9].strip()
            if len(marker) >0:
                return marker
        return None
                            
    def make_request(self):
        body = ''
        try:
            #更新时间,短连接要包括建连接时间。
            self.defineResponse.start_time = time.time()
            #如果未传入连接（短连接），则重新建立连接。
            if not self.connection:  self._init_Connection_()
            self.connection.putrequest(self.method, self.url)
            #发送头域 
            for k in self.headers.keys( ):
                self.connection.putheader( k, self.headers[k] )
            self.connection.endheaders()
            #发送body
            if len(body) >= 0: self.connection.send(self.data)            
            #接收响应
            httpResponse = self.connection.getresponse()
            #读取响应体
            body = httpResponse.read()
            #要读完才算结束
            self.defineResponse.end_time = time.time()
            self.defineResponse.status = httpResponse.status
            self.defineResponse.reason = httpResponse.reason
            #记录AK/SK            
            if  self.configs['recordAKSK'] and self.defineResponse.status == 200:
                self.recordAKSK(body)
            #从body中取requestID
            self.defineResponse.requestID = self._getReqeustidFromResponse_(body)
            logging.debug('%s %s %s\n%s\n%s' % (self.connection._http_vsn_str, self.defineResponse.status, self.defineResponse.reason, httpResponse.msg, body))
            if self.defineResponse.status >= 400:
                logging.error('response error. %s %s %s\n%s\n%s' % (self.connection._http_vsn_str, self.defineResponse.status, self.defineResponse.reason, httpResponse.msg, body))
            #若直接运行该程序，打印body内容到屏幕
            if Console_LogLevel == logging.INFO: print body
            #对于分页查询请求获取marker信息返回，供下次再次查询None代表操作不需要返回，''代表需要返回，但查找没有。
            if self._needGetMarker_():
                self.defineResponse.marker = self._getMarkerFromResponseBody_(body)
        except socket.error, (value, message):
            if Console_LogLevel == logging.INFO: print 'Error:' + message
            logging.error('socket error, Caught %d:%s. Aborting' % (value, message))
            self.defineResponse.reason = 'socket error ' + message
        except Exception, (value, message):
            logging.error('Caught %d:%s. Aborting' % (value, message))
            self.defineResponse.reason = 'error ' + message
        finally:
            if self.defineResponse.end_time == 0.0: self.defineResponse.end_time = time.time()
            #重试包括：1）对于销户等操作，如果因流控返回400，则重试，当前仅此一种情况需要重试
            if self.defineResponse.status >= 400 and  self.defineResponse.status <= 999:
                if body.find('Throttling') != -1:
                    time.sleep(.01)
                    self.defineResponse.needRetry = True 
                else:
                    #客户端错误 （非流控），需要中断操作，避免反复循环
                    self.defineResponse.marker = None
                    self.defineResponse.needRetry = False
            #只有当显示的表明长连接，才不半闭连接， 其它情况都断开连接。
            if not self.longConnection: self.close_connnection()
            #未正常接到到响应的请求。
            if self.defineResponse.status == 9999:
                self.defineResponse.requestID = '9999999999999999' 
                time.sleep(.4)
                self.defineResponse.marker = None
                #网络异常需要重建连接。
                self.defineResponse.needReconnect = True
                #网络异常本次不重试，只重建连接，保证下次请求可以成功。
                self.defineResponse.needRetry = False
            return self.defineResponse
    
def _print_usage_(Action=None):
    print 'Usage:\n    chmod +x ' + os.path.basename(__file__) 
    if Action is not None:    
        globalVar = Action[0].lower() + Action[1:]
    else:
        globalVar = ''
    UsageList = []    
    for x,y in globals().iteritems():
        if (globalVar !='' and x == globalVar + 'Fields') or (globalVar =='' and x.endswith('Fields')):
            opUsg = '    ./'+ os.path.basename(__file__) + ' --Action=' + y[0]
            for i in range(2,y[1]+2):
                opUsg += ' --' + y[i] +'=*'
            for i in range(y[1]+2, len(y)):
                opUsg += ' [--' + y[i] +'=*]'
            UsageList.append(opUsg)    
    UsageList.sort()        
    for element in UsageList:
        print(element) 
    print "\nOptional options:"
    mergDict = dict(Configs.items() + Params.items())
    for key in sorted(mergDict.keys()):
        if mergDict[key] == '': mergDict[key] = '*'
        print '    --' + key + '=' + str(mergDict[key]) 
    print '    -r,--record     Record Ak/Sk to file users.dat for actions like CreateAccountWithAll, CreateAccessKey'
    print '    -s,--silence    Silent mode. Don\'t show messages on the console.'
    print '    -v,--version    Show version.'
    print '    -h,--help       Show help.'
    print '\nTips:'
    print '    1.You can change \'AWSAccessKeyId\', \'AWSSecretKey\', \'Host\', \'Https\', \'SslVersion\' in the first part of iamPyCmd.py or using --AWSAccessKeyId=xx, --AWSSecretKey=xx, --Host=xx, --Https=xx, --SslVersion=xx'
    print '    2.Use \'\' when there is \"" or {} in the value, for example: '
    print '     --PolicyDocument=\'{"Quota":{"StorageQuota":"0"},"Dedup":{"Flag":false},"Compress":{"Flag":false},"Redundancy":{"RedupType":"EC","GroupSize":"12","ParityNumber":"3"}}\''
    print '    3.Example to create batch of accounts:'
    print '        1) (Optional) Find & Modify USER_FORMAT in this file where and how the account info stored'
    print '        2) Run \"for((i=0;i<10;i++)) do ./iamPyCmd.py -r --Action=CreateAccountWithAll --AccountName=account.$i --AccountId=id.account.$i;done;\"'
    print '        3) Check the result in user.dat'
        
import sys
if __name__ == '__main__':
    version = '\033[0;33;40m\n%s\033[0m' % ('*******' + os.path.basename(__file__)  +  ' v20160918 *******')
    #init Logging
    initLocalLogging()
    
    #load param
    param = {};config = {};aug = []
    for arg in sys.argv[1:]:
        if arg.startswith('--') and arg.find('=') != -1: 
            if arg.split('=')[0][2:] in Params.keys():
                param[arg.split('=')[0][2:]] = arg[len(arg.split('=')[0])+1:]
            if  arg.split('=')[0][2:] in Configs.keys():
                config[arg.split('=')[0][2:]] = arg[len(arg.split('=')[0])+1:]
                if arg.split('=')[0][2:] == 'Https': 
                    config['Https'] = True if config['Https'].lower() == 'true' else False #修改Https为bool
                    print 'HTTPS: %s' % config['Https']
            else:
                param[arg.split('=')[0][2:]] = arg[len(arg.split('=')[0])+1:]
        else:
            aug.append(arg)
    
    if '-v' in aug or '--version' in aug:
        print version
        sys.exit()
    if 'Action' not in param.keys():
        logging.error('no Action specified, exit')
        _print_usage_(None)
        sys.exit()
    elif 'Action' in param.keys() and ('-h' in aug or '--help' in aug):
        _print_usage_(param['Action'])
        sys.exit()
    if '-s' in aug or '--silence' in aug:
        Console_LogLevel = None
        logging.info('shutdown console out')
    if '-r' in aug or '--record' in aug:
        config['recordAKSK'] = True
    else: config['recordAKSK'] = False
    rest = IAMRest(param, config)
    try:
        rest.operate()
    except Exception,e:
        if Console_LogLevel == logging.INFO: 
            print '[ERROR]Request Error: ', e
        logging.error('exception captured [%s]' %e)
    print version