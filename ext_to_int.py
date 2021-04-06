import boto3
import sys, os

parentddir = os.path.abspath('/cp-etl-apps/cp_elt_pipeline/automation_framework/main/python/utils')
sys.path.append(parentddir)
import config_utils as config_utils
# from secret_manager import get_secret_passwd
# from utils.secret_manager import get_secret_passwd
from secret_manager import get_secret_passwd



class extToLocal(object):
    def __init__(self, common_ini_filepath, subjectarea_configPath, lc):
        self.lc = lc
        self.subjectarea_configPath = subjectarea_configPath
        self.common_ini_filepath = common_ini_filepath
        self.aws_details = config_utils.readCommon(common_ini_filepath)
        self.literals = config_utils.readLiterals(self.subjectarea_configPath)
        Main_Folder = self.subjectarea_configPath[:self.subjectarea_configPath.index("ini")]
        self.target_s3_file_upload_bucket = self.aws_details['cp_int_bucket']
        self.target_s3_file_upload_folder = self.literals['INT_BUCKET_FOLDER'] + '/'
        self.access_key = get_secret_passwd.get_secret_pass(self.literals['EXTERNAL_BUCKET_ACCESSID'])
        self.secret_access = get_secret_passwd.get_secret_pass(self.literals['EXTERNAL_BUCKET_SECRETACCESSID'])
        self.source_bucket = self.literals['EXTERNAL_BUCKET']
        self.source_bucket_key = self.literals['YYYYMMDD'] + '/'

    def ext_To_Int_bucketTransfer(self):
        source_client = boto3.client(
            's3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_access)
        response = source_client.list_objects(
            Bucket=self.source_bucket,
            Prefix=self.source_bucket_key)
        target_folder=self.target_s3_file_upload_folder
        for item in response['Contents']:
            self.target_s3_file_upload_folder=target_folder
            S3_key = item['Key']
            self.target_s3_file_upload_folder = self.target_s3_file_upload_folder + S3_key
            self.source_bucket_key = S3_key

            source_response = source_client.get_object(
            Bucket=self.source_bucket,
            Key=self.source_bucket_key)

            destination_client = boto3.client('s3')
        
            if self.literals['ENCRYPTION_TYPE'] == 'AES256' :
               destination_client.upload_fileobj(
                source_response['Body'],
                self.target_s3_file_upload_bucket,
                self.target_s3_file_upload_folder,
                ExtraArgs={"ServerSideEncryption": "AES256"})
            else :
                destination_client.upload_fileobj(
                source_response['Body'],
                self.target_s3_file_upload_bucket,
                self.target_s3_file_upload_folder)
            

