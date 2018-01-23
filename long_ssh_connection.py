# -*- coding:utf-8 -*-
import traceback
import paramiko
import time
import logging


class LongSSHConnection:
    """
    This SSH class is used for keeping the session, for example, keep a different user and interact with shell.
    It's not used for stdin, stdout and stderr. So the return is different from single execution, you have to deal with
    the return string by yourself.

    Author: b00412650
    """
    def __init__(self, node, username=None, password=None):
        self.ip = node.localIP
        if username is None:
            self.username = node.username
        else:
            self.username = username
        if password is None:
            self.password = node.password
        else:
            self.password = password
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.chan = None
        self._open_channel()

    def _open_channel(self, timeout=2):
        """
        Open a ssh shell channel, for initialization.
        :param timeout: time to verify the channel
        :return:
        """
        timeout = int(timeout)
        try_times = 1
        max_try_times = 2

        while try_times <= max_try_times:
            try:
                self.ssh.connect(self.ip, 22, self.username, self.password, timeout=timeout)
                self.ssh.get_transport().set_keepalive(30)
                self.chan = self.ssh.invoke_shell('ssh', 500, 500)
                # self.chan.send('\n')
                start_time = time.time()
                current_time = time.time()
                while current_time - start_time < timeout:
                    time.sleep(0.5)
                    if self.chan.recv_ready():
                        self.chan.recv(65535)
                        break
                    current_time = time.time()
                    if current_time - start_time >= timeout:
                        self.close()
                        logging.error('ssh receive from host %s timeout' % self.ip)
                        raise Exception('ssh receive from host %s timeout' % self.ip)
                break
            except Exception:
                print Exception.message
                if try_times == max_try_times:
                    raise Exception('ssh connect to host %s timeout' % self.ip)
                time.sleep(2)
                try_times += 1
                logging.info('try connect %d times' % try_times)

    def execute_cmd(self, cmd, expect_end='#', timeout=30):
        """
        Execute a cmd. After execution, the ssh channel won't lose, which means this can switch and keep a user, or
        something like it.
        This method can also interact with the ssh, but you have to provide the expect end sign, if the end is not '#'.
        :param cmd: directly send a cmd.
        :param expect_end: If the expect_end is reached in the whole result, return the result.
        :param timeout: After timeout seconds, return the result, no matter if expect_end is reached.
        :return: result, contains the PS1 sign(for example [root@localhost ~]#).
        """
        timeout = int(timeout)
        result = ''
        start_time = time.time()
        try:
            self.chan.send(cmd + '\n')
            while True:
                time.sleep(0.5)
                if self.chan.recv_ready():
                    ret = self.chan.recv(65535)
                    logging.info(ret)
                    result += str(ret)
                current_time = time.time()
                if result.strip().endswith(expect_end) or current_time - start_time >= timeout:
                    start_index = len(cmd + '\r\n')
                    return result.strip()[start_index:]
        except Exception:
            logging.warn('IP: ' + self.ip + '\n' + traceback.format_exc())

    def close(self):
        if self.chan is not None:
            self.chan.close()
        self.ssh.close()
