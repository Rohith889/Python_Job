import pysftp
import paramiko
import os
import fire
#import utils.config_utils as config_utils
import sys
sys.path.append("/cp-etl-apps/cp_elt_pipeline/automation_framework/main/python/utils")
import config_utils
from logger import LoggerClient



"""
python3 test_sftp.py sftp
--subjectarea_configPath=/cp-etl-apps/cp_elt_pipeline/automation_framework/main/python/ini/partycity/partycity_dml.ini
"""

class sftpToLocal(object):
    def __init__(self, subjectarea_configPath,lc) :
        self.lc = lc
        self.subjectarea_configPath = subjectarea_configPath
        self.literals = config_utils.readLiterals(self.subjectarea_configPath)
        self.HOSTNAME = self.literals['HOSTNAME']
        self.USERNAME = self.literals['USERNAME']
        self.IDENTITY_FILE = self.literals['IDENTITY_FILE']
        Main_Folder = self.subjectarea_configPath[:self.subjectarea_configPath.index("ini")]
        self.LOCAL_IN_DIR = Main_Folder + self.literals['LOCAL_IN_DIR'] + '/' + self.literals['YYYYMMDD']

    def sftp(self):
        mykey = paramiko.RSAKey.from_private_key_file(self.IDENTITY_FILE)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.HOSTNAME, username=self.USERNAME, pkey=mykey)
        sftp = ssh.open_sftp()
        dir_items = sftp.listdir("/data")
        for file in dir_items:
            # Check for Data directory
            if not os.path.exists(self.LOCAL_IN_DIR):
                access_rights = 0o777
                os.mkdir(self.LOCAL_IN_DIR, mode=access_rights)

            file_remote = "/data/" + file
            file_local = self.LOCAL_IN_DIR + '/' + file
            self.lc.logfile.info("------- Tranfering file from remote {} >>> {} ".format(file_remote, file_local))
            sftp.get(file_remote, file_local)
        

if __name__ == '__main__':
    fire.Fire(sftp)