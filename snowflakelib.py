#!/usr/bin/env python3
__author__ = 'Anubha'

import snowflake.connector
from snowflake.connector import connect
from snowflake.connector import DictCursor
import sys, getopt, os
from os import path
import datetime
from datetime import date
import time
import logging

parentddir = os.path.abspath('/cp-etl-apps/cp_elt_pipeline/automation_framework/main/python/module')
sys.path.append(parentddir)
from CP_lib import CP_Client

parentddir = os.path.abspath('/cp-etl-apps/cp_elt_pipeline/automation_framework/main/python/utils')
sys.path.append(parentddir)
import sql_utils as sql_utils
from logger import LoggerClient
from secret_manager import get_secret_passwd


class Snowflake_lib(object):

    def __init__(self, Snowflake_Dev, aws_details, lc):
        self.lc = lc
        self.Snowflake_Dev = Snowflake_Dev
        self.aws_details = aws_details
        self.account = self.Snowflake_Dev['account']
        self.SNOW_USER = self.Snowflake_Dev['snow_user']
        self.SNOW_PASS = get_secret_passwd.get_secret_pass(self.Snowflake_Dev['snow_pass_key'])
        self.ctx = snowflake.connector.connect(
            user=self.SNOW_USER,
            password=self.SNOW_PASS,
            account=self.account,
            role=self.Snowflake_Dev['role'],
            warehouse=self.Snowflake_Dev['warehouse'],
            database=self.Snowflake_Dev['db'],
            schema=self.Snowflake_Dev['landing_schema'])

        self.cs = self.ctx.cursor(DictCursor)

    def results_handler(self, res_dict):
        res_dict = res_dict[0]
        rows_ins_key = 'number of rows inserted'
        rows_upd_key = 'number of rows updated'
        rows_del_key = 'number of rows deleted'
        rows_inserted = 0
        rows_updated = 0
        rows_deleted = 0

        row_result = {}

        if 'status' in res_dict and res_dict['status'] == 'LOADED':
            rows_inserted = res_dict['rows_loaded']
            rows_updated = 0
            rows_deleted = 0
        elif rows_ins_key in res_dict.keys() and rows_upd_key in res_dict.keys():
            rows_inserted = res_dict[rows_ins_key]
            rows_updated = res_dict[rows_upd_key]
            rows_deleted = 0
        elif rows_ins_key in res_dict.keys():
            rows_inserted = res_dict[rows_ins_key]
            rows_updated = 0
            rows_deleted = 0
        elif rows_upd_key in res_dict.keys():
            rows_inserted = 0
            rows_updated = res_dict[rows_upd_key]
            rows_deleted = 0
        elif rows_del_key in res_dict.keys():
            rows_inserted = 0
            rows_updated = 0
            rows_deleted = res_dict[rows_del_key]

        row_result['rows_inserted'] = rows_inserted
        row_result['rows_updated'] = rows_updated
        row_result['rows_deleted'] = rows_deleted

        return row_result

    def sf_execution(self, subjectarea, script_path, logSQLfile, elt_batch_id, literals=None,
                     aws_details=None):
        """Executes SQL in snowflake with parameters and identifiers. And logs the result in Log table as well as Log file
           Args - cs : snowflake cursor
                  subjectarea : Job Name
                 script_path : SQL script path
                 database , schema : identifier values
                 logSQLfile : SQL path for insert statement in Log table
                 logfilename : Log file location
                 literals : Literals for SQL defined in the ini file
           Returns - Executes the SQL statement in snowflake
                     Logs result in Log table and Log file
        """

        snowsql_startTime = time.time()
        snowsql_endTime = time.time()
        sqlScriptName = str(script_path[script_path.rindex("/") + 1:])
        identifiers = {**self.Snowflake_Dev, **self.aws_details}
        if os.path.isfile(script_path):
            sqlFile = sql_utils.read_sql(script_path)
            for commands in sqlFile:
                try:
                    query = commands.format(**identifiers)
                    print("query",query)
                    if literals is None:
                        self.cs.execute(query)
                    else:
                        literals = {**literals, **elt_batch_id}
                        self.cs.execute(query % literals)
                    

                    sf_query_output = self.cs.fetchall()
                    results = self.results_handler(sf_query_output)
                    row_insert = str(results['rows_inserted'])
                    row_update = str(results['rows_updated'])
                    row_delete = str(results['rows_deleted'])

                    message = str(sf_query_output[0])
                    snowsql_endTime = time.time()
                    Total_Exec_Time = str(round((snowsql_endTime - snowsql_startTime), 2))

                    # ----------------- Log ---------------------------------------------------------------------------
                    # log the results in CP_ACTIVITY_DETAIL_LOG
                    log_type = "LOG"
                    log_CP_Client = CP_Client(self.cs, logSQLfile, identifiers, log_type, elt_batch_id,
                                              subjectarea, sqlScriptName, row_insert, row_update, row_delete,
                                              message, snowsql_startTime, snowsql_endTime)
                    log_CP_Client.insertActivity()

                    self.lc.logfile.info('\n')
                    self.lc.logfile.info(query % literals + '\n' + "Executed Successfully In " + Total_Exec_Time + ' seconds \n')
                    self.lc.logfile.info('rows effected: ' + row_insert)


                except snowflake.connector.errors.ProgrammingError as e:

                    # Log Error details in the CP_ACTIVITY_DETAIL_LOG table.
                    log_type = "ERROR"
                    log_CP_Client = CP_Client(self.cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea,
                                              sqlScriptName,
                                              row_insert, row_update, row_delete, message, snowsql_startTime,
                                              snowsql_endTime, e.errno, e.msg, e.sqlstate)

                    log_CP_Client.insertActivity()

                    logging.error('\n')
                    logging.error('CRITICAL ERROR Executing the below SQL: \n' + commands + '\n')
                    logging.error(format(e))
                    logging.error('Error {0} ({1}): {2} ({3}) \n'.format(e.errno, e.sqlstate, e.msg, e.sfqid))

                    sys.exit("CRITICAL ERROR WHILE EXECUTING THE SQL -> " + e.msg)

        else:
            log_type = "ERROR"
            log_CP_Client = CP_Client(self.cs, logSQLfile, identifiers, log_type, elt_batch_id, subjectarea,
                                      sqlScriptName,
                                      0, 0, 0, None, snowsql_startTime, snowsql_endTime)
            log_CP_Client.insertActivity()

            logging.error('\n')
            logging.error('CRITICAL ERROR : \n' + self.script_path + '\n')
            logging.error("File Not Found Error : \n")
            logging.error('No such file or directory : ' + self.script_path)

            sys.exit("CRITICAL ERROR  -> " + script_path + ' Does Not Exist in the Source directory..')
