# -*- coding:utf-8 -*-
import random
import string
import base64
import hmac
import hashlib
import logging
import sys
import time
import os

TIME_FORMAT = '%a, %d %b %Y %H:%M:%S GMT'
ISO8601 = '%Y%m%dT%H%M%SZ'
ISO8601_MS = '%Y-%m-%dT%H:%M:%S.%fZ'
RFC1123 = '%a, %d %b %Y %H:%M:%S %Z'


class InitSSHRemoteHost:
    def __init__(self, ip, username, password, tools_path, tool_number):
        self.localIP = ip
        self.username = username
        self.password = password
        self.tools_path = tools_path
        self.tool_number = tool_number


def random_string_create(string_length):
    if isinstance(string_length, int):
        return ''.join(
            [random.choice(string.ascii_letters + string.digits + string.punctuation.translate(None, '!,%&<>\'\\^`')) for n in
             range(string_length)])
    else:
        print 'input error'


def generate_image_format(image_format):
    """
    生成图片转码格式
    :param image_format: 
    :return: 
    """
    if str(image_format).find(',') != -1:
        format_array = image_format.split(',')
        return format_array[random.randint(0, len(format_array) - 1)]

    return image_format


def generate_a_size(data_size_str):
    """
    返回对象大小，和是否是固定值，可必免反复请求。ifFixed = True
    :param data_size_str: 
    :return: 
    """
    if str(data_size_str).find('~') != -1 and str(data_size_str).find(',') != -1:
        size_array = data_size_str.split(',')
        size_chosen = size_array[random.randint(0, len(size_array) - 1)]
        start_size = int(size_chosen.split('~')[0])
        end_size = int(size_chosen.split('~')[1])
        return random.randint(start_size, end_size), False
    elif str(data_size_str).find('~') != -1:
        start_size = int(data_size_str.split('~')[0])
        end_size = int(data_size_str.split('~')[1])
        return random.randint(start_size, end_size), False
    elif str(data_size_str).find(',') != -1:
        size_array = data_size_str.split(',')
        return int(size_array[random.randint(0, len(size_array) - 1)]), False
    else:
        return int(data_size_str), True


def get_utf8_value(value):
    if not value:
        return ''
    if isinstance(value, str):
        return value
    if isinstance(value, unicode):
        return value.encode('utf-8')
    return str(value)


def compare_version(v1, v2):
    v1 = v1.split('.')
    v2 = v2.split('.')
    try:
        for i in range(0, len(v1)):
            if len(v2) < i + 1:
                return 1
            elif int(v1[i]) < int(v2[i]):
                return -1
            elif int(v1[i]) > int(v2[i]):
                return 1
    except:
        return -1
    if len(v2) > len(v1):
        return -1
    return 0


def generate_config_file(config_file):
    """
    generate specific configuration file
    :param config_file: 
    :return: config generated
    """
    config = {}
    try:
        f = open(config_file, 'r')
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            if line and line[0] != '#':
                config[line[:line.find('=')].strip()] = line[line.find('=') + 1:].strip()
            else:
                continue
        f.close()
    except Exception, e:
        print '[ERROR] Read config file %s error: %s' % (config_file, e)
        sys.exit()

    return config


def read_distribute_config(config_file='distribute_config.dat'):
    """
    read given distribute file configuration
    :param config_file: 
    :return: 
    """
    config = generate_config_file(config_file)

    config['Slaves'] = config['Slaves'].replace(' ', '').replace(',,', ',')
    config['Usernames'] = config['Usernames'].replace(' ', '').replace(',,', ',')
    config['Passwords'] = config['Passwords'].replace(' ', '').replace(',,', ',')
    config['Toolpaths'] = config['Toolpaths'].replace(' ', '').replace(',,', ',')
    config['ToolNumberPerServer'] = config['ToolNumberPerServer'].replace(' ', '').replace(',,', ',')

    if config['Master'] is not None and config['Master'] and \
                    config['Slaves'] is not None and config['Slaves'] and \
                    config['Usernames'] is not None and config['Usernames'] and \
                    config['Passwords'] is not None and config['Passwords'] and \
                    config['Toolpaths'] is not None and config['Toolpaths'] and \
                    config['ToolNumberPerServer'] is not None and config['ToolNumberPerServer'] and \
                    config['RunTime'] is not None and config['RunTime']:
        pass
    else:
        raise Exception('Some config(s) is missed')

    return config


def generate_slave_servers(config):
    """
    initialize slave servers
    :param config: distribute configuration
    :return: generated slave servers
    """
    slaves = []
    slave_ips = config['Slaves'].split(',')
    slave_usernames = config['Usernames'].split(',')
    slave_passwords = config['Passwords'].split(',')
    slave_tool_paths = config['Toolpaths'].split(',')
    slave_tool_numbers = config['ToolNumberPerServer'].split(',')

    k = 0
    for i in xrange(len(slave_ips)):
        for j in xrange(int(slave_tool_numbers[i])):
            ip = slave_ips[i]
            username = slave_usernames[i] if len(slave_usernames) > 1 else slave_usernames[0]
            password = slave_passwords[i] if len(slave_passwords) > 1 else slave_passwords[0]
            tool_path = slave_tool_paths[k]
            k += 1
            tool_number = "1"
            slaves.append(InitSSHRemoteHost(ip, username, password, tool_path, tool_number))

    return slaves


def generate_connections(servers):
    """
    generate provided servers' connections
    :param servers: 
    :return: 
    """
    from long_ssh_connection import LongSSHConnection

    connects = []
    for server in servers:
        connect = LongSSHConnection(server)

        # build the connection to provided server
        logging.debug("Build connection to server[%s]" % server.localIP)
        r = connect.execute_cmd('ssh %s@%s' % (server.username, server.localIP), timeout=10)
        if r.endswith('?'):
            connect.execute_cmd('yes', expect_end=':')
        connect.execute_cmd(server.password, expect_end='#')
        logging.debug("Successfully built the connection to server[%s]" % server.localIP)

        # go to provided tool path
        logging.debug("Go to provided tool path[%s] of server[%s]" % (server.tools_path, server.localIP))
        connect.execute_cmd('cd %s' % server.tools_path, timeout=5)

        connects.append(connect)

    return connects


def get_brief_file_name(connect):
    """
    get brief file name
    :param connect: 
    :return: 
    """
    logging.warn("try to get brief file from server: %s" % connect.ip)
    get_slave_brief_file_name_result = connect.execute_cmd(r"ls -t result/*_brief.txt | head -1")
    tmp = get_slave_brief_file_name_result.split('\r\n')[0]

    return tmp.split('/')[1]


def start_tool(connect, test_case, run_time):
    """
    start tool in server
    :param connect:
    :param test_case:
    :param run_time:
    :return:
    """
    print "Start at %s, send run signal to slave[%s]" % (time.strftime('%X %x %Z'), connect.ip)
    logging.warn("send run signal to server %s" % connect.ip)
    connect.execute_cmd('python run.py %s' % test_case, timeout=10)


def convert_time_format_str(time_sec):
    if time_sec < 0:
        return '--\'--\'--'
    if time_sec >= 8553600:
        return '>99 days'
    elif time_sec >= 86400:
        return '%2.2d Days %2.2d\'%2.2d\'%2.2d' % (
            time_sec / (3600 * 24), time_sec % (3600 * 24) / 3600, (time_sec % 3600 / 60), (time_sec % 60))
    else:
        ms = time_sec - int('%2.2d' % (time_sec % 60))
        return '%2.2d\'%2.2d\'%2.2d.%d' % (time_sec / 3600, (time_sec % 3600 / 60), (time_sec % 60), ms * 1000)


def generate_response(response):
    """
    response of server always contains "\r\n", need to remove it
    :param response: response of server
    :return: 
    """
    if response is not None:
        resp = response.split('\r\n')
        resp = resp[0]
        return resp
    else:
        raise Exception("response of server is none, please confirm it.")


def convert_to_size_str(size_bt):
    kb = 2 ** 10
    mb = 2 ** 20
    gb = 2 ** 30
    tb = 2 ** 40
    pb = 2 ** 50
    if size_bt >= 100 * pb:
        return '>100 PB'
    elif size_bt >= pb:
        return "%.2f PB" % (size_bt / (pb * 1.0))
    elif size_bt >= tb:
        return "%.2f TB" % (size_bt / (tb * 1.0))
    elif size_bt >= gb:
        return "%.2f GB" % (size_bt / (gb * 1.0))
    elif size_bt >= mb:
        return "%.2f MB" % (size_bt / (mb * 1.0))
    elif size_bt >= kb:
        return "%.2f KB" % (size_bt / (kb * 1.0))
    else:
        return "%.2f B" % size_bt
