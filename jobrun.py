#!/usr/bin/env python3
__author__ = 'Anubha'

import snowflake.connector
from snowflake.connector import connect
from snowflake.connector import DictCursor
import sys, getopt, os, shutil
from os import path
import datetime
from datetime import date
import time
import boto3
import base64
import json
import ast
import collections
import pandas as pd
import xlrd
#import html5lib
from botocore.exceptions import ClientError
import sqlparse
from sqlparse import tokens
from itertools import chain
import logging

parentddir = os.path.abspath('/cp-etl-apps/cp_elt_pipeline/automation_framework/main/python/module/')
sys.path.append(parentddir)
from CP_lib import CP_Client
from aws_lib import aws_lib
from sftp_to_local import sftpToLocal
from snowflake_lib import Snowflake_lib
from retail_link_download import retailLinkDownload
from itemshare_download import itemsharedownload
parentddir = os.path.abspath('/cp-etl-apps/cp_elt_pipeline/automation_framework/main/python/utils/')
sys.path.append(parentddir)
import sql_utils as sql_utils
import config_utils as config_utils
from gzip_utils import gzipComp
from xls_to_txt import xlsToTxt
from logger import LoggerClient
from secret_manager import get_secret_passwd
from ext_to_int import extToLocal


def main(argv):
    # -------------- Job Arguements ---------------------------------------
    subjectarea = str(sys.argv[1]).upper()
    second_arg = str(sys.argv[2])

    # -------------- Main Folder Path ---------------------------------------
    Main_Folder = '/cp-etl-apps/cp_elt_pipeline/automation_framework/main/python/'
    common_ini_filepath = Main_Folder + 'ini/' + 'common.ini'

    # ---------------- Ini File Check ----------------------------------------
    if os.path.exists(Main_Folder + 'ini/'):
        if os.path.isfile(common_ini_filepath):
            pass
        else:
            sys.exit("CRITICAL ERROR -> Missing ini file for common ")
    else:
        sys.exit("CRITICAL ERROR -> Missing ini directory ")

    try:
        folder_structure = config_utils.getConfigSection(common_ini_filepath, "FOLDER_STRUCTURE")
        Snowflake_Dev = config_utils.getConfigSection(common_ini_filepath, "Snowflake_Dev")
        sqldir = folder_structure['sqldir']
        logdir = folder_structure['logdir']
        logSQLfile = folder_structure['logsql']
        account = Snowflake_Dev['account']
        SNOW_USER = Snowflake_Dev['snow_user']
        SNOW_PASS = get_secret_passwd.get_secret_pass(Snowflake_Dev['snow_pass_key'])
	
        aws_details = config_utils.getConfigSection(common_ini_filepath, "AWS_DETAILS")
        cp_int_bucket = aws_details['cp_int_bucket']
        cp_arc_bucket = aws_details['cp_arc_bucket']
        encryption_type = aws_details['int_bucket_encryption_type']
        kms_key = get_secret_passwd.get_secret_pass(aws_details['int_bucket_key'])

    except Exception as e:
        sys.exit("CRITICAL ERROR -> Missing or incorrect details in the common ini file ")

    # ----------- Start log --------------------------------------------------------------
    try:
        lc = LoggerClient(logdir, subjectarea, second_arg)
        lc.logfile.info('JOB Execution START TIME:' +
                        time.strftime('%Y-%m-%d %I:%M:%S %p'))
    except Exception as err:
        logging.error('Unable to start logger. Error: {}.'.format(str(err)))
        exit(1)

    # ----------- Snowflake query for etl_batch_id --------------------------------------
    sf = Snowflake_lib(Snowflake_Dev, aws_details, lc)
    cs = sf.ctx.cursor(DictCursor)
    batch_query = "select {db}.{landing_schema}.elt_batch_seq.nextval as ELT_BATCH_ID;".format(**Snowflake_Dev)
    print(batch_query)
    elt_batch_id = cs.execute(batch_query).fetchall()[0]
    lc.logfile.info("Batch ID Created : " + str(elt_batch_id['ELT_BATCH_ID']))

    # ------------ Log entry to CP_ACTIVITY_DETAIL_LOG table --------------------------
    log_type = "LOG"
    snowsql_startTime = time.time()
    snowsql_endTime = time.time()
    identifiers = {**Snowflake_Dev, **aws_details}
    log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, "JOB INITIATION", 0, 0,
                              0, "INITIATING JOB WITH ARGUEMENT : " + second_arg, snowsql_startTime, snowsql_endTime)
    log_CP_Client.insertActivity()

    # ------------- Executing .sql file ----------------------------------
    if ".sql" in second_arg:
        lc.logfile.info("Executing sql : " + subjectarea + " " + second_arg)
        arg_folder_path = second_arg.split("/")[0].upper()
        sql_name = second_arg.split("/")[1]
        script_path = sqldir + subjectarea + "/" + arg_folder_path + "/" + sql_name
        print(script_path)
        subjectarea_configPath = Main_Folder + 'ini/' + subjectarea + '/' + subjectarea + "_" + arg_folder_path.lower() + ".ini"
        literals = config_utils.readLiterals(subjectarea_configPath)
        # -------------- SQL SCRIPT PATH CHECK -----------------------------
        if not os.path.exists(script_path):
            log_type = "ERROR"
            snowsql_endTime = time.time()
            log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg, 0,
                                      0, 0,
                                      'CRITICAL ERROR -> ' + script_path + ' Does Not Exist in the Source directory..',
                                      snowsql_startTime, snowsql_endTime, '',
                                      'CRITICAL ERROR -> ' + script_path + ' Does Not Exist in the Source directory..')
            log_CP_Client.insertActivity()
            sys.exit('CRITICAL ERROR -> ' + script_path + ' Does Not Exist in the Source directory..')
        sf_exec = Snowflake_lib(Snowflake_Dev, aws_details, lc)
        sf_exec.sf_execution(subjectarea, script_path, logSQLfile, elt_batch_id, literals)

    else:
        if ".ini" in second_arg:
            subjectarea_configPath = Main_Folder + 'ini/' + subjectarea.upper() + '/' + second_arg
            ini_flag = True
        else:
            subjectarea_configPath = Main_Folder + 'ini/' + subjectarea.upper() + '/' + subjectarea.upper() + "_dml.ini"
            ini_flag = False

        # ----------- Check existence of subjectarea_configPath -------------------------
        print(subjectarea_configPath)
        if os.path.isfile(subjectarea_configPath):
            pass
        else:
            sys.exit("CRITICAL ERROR -> Missing ini file for subjectarea " + subjectarea)

        # -------- Sort script under SQL in ini file based on execution order ---------------
        sql_df = pd.DataFrame()
        if "SQL" not in config_utils.checkConfigSection(subjectarea_configPath):
            script = None
        else:
            script = ast.literal_eval(config_utils.getConfig(subjectarea_configPath, "SQL", "SCRIPT"))
            sql_df = pd.DataFrame(script)
            sql_df.sort_values(by=["execution_order"], inplace=True)
            sql_script = sql_df.script.tolist()

        literals = config_utils.readLiterals(subjectarea_configPath)

        if (ini_flag and literals['ENABLE_EXTERNAL_TO_INT_BUCKET_TRANSFER'] == 'Y') or "ext_to_int" == second_arg.lower():
            snowsql_startTime = time.time()
            try:
                lc.logfile.info('---------copying files from external s3 to Integration bucket----------')
                ext_to_int_bucket_transfer = extToLocal(common_ini_filepath, subjectarea_configPath, lc)
                ext_to_int_bucket_transfer.ext_To_Int_bucketTransfer()

                log_type = "LOG"
                snowsql_endTime = time.time()
                log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg,
                                          0, 0, 0, "COPYING FILES FROM EXTERNAL TO INTEGRATION S3 BUCKET",
                                          snowsql_startTime, snowsql_endTime)
                log_CP_Client.insertActivity()

            except Exception as err:
                log_type = "ERROR"
                snowsql_endTime = time.time()
                log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg,
                                          0, 0, 0, str(err), snowsql_startTime, snowsql_endTime, '', str(err))
                log_CP_Client.insertActivity()

        if (ini_flag and literals['ENABLE_RETAIL_LINK_DOWNLOAD'] == 'Y') or "report_download" == second_arg.lower():
            snowsql_startTime = time.time()
            try:
                lc.logfile.info("----------------- Initiating download from Retail Link ------------------------")
                retail_link_Download = retailLinkDownload(subjectarea_configPath, lc)
                
                download_flag=retail_link_Download.Walmart_Datapull()
                if download_flag is False:
                    
                    log_type = "ERROR"
                    snowsql_endTime = time.time()
                    
                    log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg,
                                          0, 0, 0, "Element Not Found", snowsql_startTime, snowsql_endTime, '', "Element Not Found")
                    
                    log_CP_Client.insertActivity()
                                        
                else:
                    log_type = "LOG"
                    snowsql_endTime = time.time()
                    log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea,
                                              second_arg,
                                              0, 0, 0, "WMT RETAIL LINK DOWNLOAD", snowsql_startTime, snowsql_endTime)
                    log_CP_Client.insertActivity()
            except Exception as err:
                log_type = "ERROR"
                snowsql_endTime = time.time()
                log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg,
                                          0, 0, 0, str(err), snowsql_startTime, snowsql_endTime, '', str(err))
                log_CP_Client.insertActivity()

        if (ini_flag and literals['ENABLE_ITEMSHARE_DOWNLOAD'] == 'Y') or "itemshare_download" == second_arg.lower():
            print("----------Retail link Itemshare Download-------------------------")
            snowsql_startTime = time.time()
            try:
                lc.logfile.info("----------------- Initiating ItemShare download from Retail Link ------------------------")
                itemshare_Download = itemsharedownload(Main_Folder, subjectarea,lc)
                log_type = "LOG"
                snowsql_endTime = time.time()
                log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg,
                                          0, 0, 0, "WMT ITEMSHARE DOWNLOAD", snowsql_startTime, snowsql_endTime)
                log_CP_Client.insertActivity()
                lc.logfile.info("----------------- Completed ItemShare download from Retail Link ------------------------")           
            except Exception as err:
                log_type = "ERROR"
                snowsql_endTime = time.time()
                log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg,
                                          0, 0, 0, str(err), snowsql_startTime, snowsql_endTime, '', str(err))
                log_CP_Client.insertActivity()
                er=str(err)
                lc.logfile.info("ERROR:=====> " +er)
                
        
        if (ini_flag and literals['ENABLE_SFTP_TO_LOCAL_TXT_FLAG'] == 'Y') or "sftp_to_local" == second_arg.lower():
            snowsql_startTime = time.time()
            try:
                lc.logfile.info("----------- Initiating Sftp from remote to Local ----------------------")
                sftp_to_local = sftpToLocal(subjectarea_configPath, lc)
                sftp_to_local.sftp()

                log_type = "LOG"
                snowsql_endTime = time.time()
                log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg,
                                          0, 0, 0, "SFTP FILES TO LOCAL", snowsql_startTime, snowsql_endTime)
                log_CP_Client.insertActivity()

            except Exception as err:
                log_type = "ERROR"
                snowsql_endTime = time.time()
                log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg,
                                          0, 0, 0, str(err), snowsql_startTime, snowsql_endTime, '', str(err))
                log_CP_Client.insertActivity()

        if (ini_flag and literals['ENABLE_LOCAL_XLS_TO_TXT_FLAG'] == 'Y') or "xlstotxt" == second_arg.lower():
            snowsql_startTime = time.time()
            try:
                lc.logfile.info("-----------Initiating xlsx to txt Conversion -------------")
                xls_to_txt = xlsToTxt(subjectarea_configPath, lc)
                xls_to_txt.xls_to_txt_conversion()

                log_type = "LOG"
                snowsql_endTime = time.time()
                log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg,
                                          0, 0, 0, "CONVERTING XLS TO TXT", snowsql_startTime, snowsql_endTime)
                log_CP_Client.insertActivity()

            except Exception as err:
                log_type = "ERROR"
                snowsql_endTime = time.time()
                log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg,
                                          0, 0, 0, str(err), snowsql_startTime, snowsql_endTime, '', str(err))
                log_CP_Client.insertActivity()

        if (ini_flag and literals['ENABLE_GZIP_FLAG'] == 'Y') or "gzip_s3load" == second_arg.lower():
            snowsql_startTime = time.time()
            try:
                lc.logfile.info("------------ Intiating Gzip Compression ----------------------")
                gzip_Comp = gzipComp(common_ini_filepath, subjectarea_configPath, lc)
                gzip_Comp.gzip_utils()
                gzip_Comp.gzipUpload()

                log_type = "LOG"
                snowsql_endTime = time.time()
                log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg,
                                          0, 0, 0, "UPLOADING GZIP FILE IN INTEGRATION BUCKET", snowsql_startTime,
                                          snowsql_endTime)
                log_CP_Client.insertActivity()

            except Exception as err:
                log_type = "ERROR"
                snowsql_endTime = time.time()
                log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg,
                                          0, 0, 0, str(err), snowsql_startTime, snowsql_endTime, '', str(err))
                log_CP_Client.insertActivity()

        if (ini_flag and literals[
            'ENABLE_IND_FILES_TO_INT_BUCKET_TRANSFER'] == 'Y') or "ind_files_to_int" == second_arg.lower():
            snowsql_startTime = time.time()
            try:
                lc.logfile.info("------------ Intiating SLFC to Integration Transfer ----------------------")
                ind_files_to_int_bucket_transfer = aws_lib(common_ini_filepath, subjectarea_configPath, lc)
                report=literals["REPORT"]
                print("report",report)
                ind_files_to_int_bucket_transfer.ind_files_to_int_bucketTransfer(report)

                log_type = "LOG"
                snowsql_endTime = time.time()
                log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg,
                                          0, 0, 0, "COPYING FILES FROM SALESFORCE TO INTEGRATION", snowsql_startTime,
                                          snowsql_endTime)
                log_CP_Client.insertActivity()

            except Exception as err:
                log_type = "ERROR"
                snowsql_endTime = time.time()
                log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg,
                                          0, 0, 0, str(err), snowsql_startTime, snowsql_endTime, '', str(err))
                log_CP_Client.insertActivity()

        if (ini_flag and literals['ENABLE_IND_TO_INT_BUCKET_TRANSFER'] == 'Y') or "ind_to_int" == second_arg.lower():
            snowsql_startTime = time.time()
            try:
                lc.logfile.info("---------- IND to INT Transferring -------------------")
                ind_to_int_bucket_transfer = aws_lib(common_ini_filepath, subjectarea_configPath, lc)
                ind_to_int_bucket_transfer.ind_to_int_bucketTransfer()

                log_type = "LOG"
                snowsql_endTime = time.time()
                log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg,
                                          0, 0, 0,
                                          "INDIVIDUAL TO INTEGRATION BUCKET TRANSFER", snowsql_startTime,
                                          snowsql_endTime)
                log_CP_Client.insertActivity()

            except Exception as err:
                log_type = "ERROR"
                snowsql_endTime = time.time()
                log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg,
                                          0, 0, 0, str(err), snowsql_startTime, snowsql_endTime, '', str(err))
                log_CP_Client.insertActivity()

        if not os.path.exists(sqldir):
            log_type = "ERROR"
            snowsql_endTime = time.time()
            log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg, 0,
                                      0, 0,
                                      'CRITICAL ERROR -> ' + sqldir + ' Does Not Exist in the Source directory..',
                                      snowsql_startTime, snowsql_endTime, '',
                                      'CRITICAL ERROR -> ' + sqldir + ' Does Not Exist in the Source directory..')
            log_CP_Client.insertActivity()
            sys.exit('CRITICAL ERROR -> ' + sqldir + ' Does Not Exist in the Source directory..')
        else:
            if (ini_flag and not (sql_df.empty)):
                lc.logfile.info("---------- Executing SQL  -------------------")
                for sql in sql_df.itertuples():
                    script_path = sqldir + subjectarea + '/' + sql.script
                    sf_exec = Snowflake_lib(Snowflake_Dev, aws_details, lc)
                    sf_exec.sf_execution(subjectarea, script_path, logSQLfile, elt_batch_id, literals)

        if (ini_flag and literals['ENABLE_INT_TO_ARC_BUCKET_TRANSFER'] == 'Y') or "archive" == second_arg.lower():
            snowsql_startTime = time.time()
            try:
                lc.logfile.info("----------- INT to ARC Transferring ------------------")
                int_to_arc_bucket_trans = aws_lib(common_ini_filepath, subjectarea_configPath, lc)
                int_to_arc_bucket_trans.int_to_arc_bucketTransfer()

                log_type = "LOG"
                snowsql_endTime = time.time()
                log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg,
                                          0, 0, 0,
                                          "ARCHIVING INTEGRATION BUCKET", snowsql_startTime, snowsql_endTime)
                log_CP_Client.insertActivity()

            except Exception as err:
                log_type = "ERROR"
                snowsql_endTime = time.time()
                log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg,
                                          0, 0, 0, str(err), snowsql_startTime, snowsql_endTime, '', str(err))
                log_CP_Client.insertActivity()

        if (ini_flag and literals['ENABLE_PURGE'] == 'Y') or "purge" == second_arg.lower():
            snowsql_startTime = time.time()
            try:
                lc.logfile.info("-------- Purging Files from IND/INT bucket -------------------")
                purge_bucket = aws_lib(common_ini_filepath, subjectarea_configPath, lc)
                purge_bucket.int_bucket_purging()
                if (literals['CP_INDIVIDUAL_BUCKET']!="" and literals['IND_PURGE_DAYS']!=''):
                    purge_bucket.ind_bucket_purging()
                
                    
                log_type = "LOG"
                snowsql_endTime = time.time()
                log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg,
                                          0, 0, 0,
                                          "PURGING INTEGRATION/INDIVIDUAL BUCKETS", snowsql_startTime, snowsql_endTime)
                log_CP_Client.insertActivity()

                
                numdays = 86400 * 7
                now = time.time()
                if (literals['LOCAL_IN_DIR'] != ''):
                    lc.logfile.info("-------- Purging Files from Local! -------------------")
                    directory = os.path.join(Main_Folder, literals['LOCAL_IN_DIR'])
                    for r, d, f in os.walk(directory):
                        for dir in d:
                            timestamp = os.path.getmtime(os.path.join(r, dir))
                            if now - numdays > timestamp:
                                shutil.rmtree(os.path.join(r, dir))

            except Exception as err:
                log_type = "ERROR"
                snowsql_endTime = time.time()
                log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg,
                                          0, 0, 0, str(err), snowsql_startTime, snowsql_endTime, '', str(err))
                log_CP_Client.insertActivity()

        if (ini_flag and literals['ENABLE_PURGE_ARCHIVE'] == 'Y') or "purge_archive" == second_arg.lower():
            snowsql_startTime = time.time()
            try:
                lc.logfile.info("-------- Purging Files from Archive bucket -------------------")
                purge_bucket = aws_lib(common_ini_filepath, subjectarea_configPath, lc)
                purge_bucket.arc_bucket_purging()

                log_type = "LOG"
                snowsql_endTime = time.time()
                log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg,
                                          0, 0, 0,
                                          "PURGING FILES FROM ARCHIVE BUCKETS", snowsql_startTime, snowsql_endTime)
                log_CP_Client.insertActivity()

            except Exception as err:
                log_type = "ERROR"
                snowsql_endTime = time.time()
                log_CP_Client = CP_Client(cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea, second_arg,
                                          0, 0, 0, str(err), snowsql_startTime, snowsql_endTime, '', str(err))
                log_CP_Client.insertActivity()


if __name__ == "__main__":
    main(sys.argv)