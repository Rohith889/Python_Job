#!/usr/bin/env python3
__author__ = 'Anubha'

import snowflake.connector
from snowflake.connector import connect
from snowflake.connector import DictCursor
import sys, getopt, os
from os import path
from configparser import ConfigParser
import datetime
from datetime import date
import time
import boto3
import base64
import json
import ast
import pandas as pd
from botocore.exceptions import ClientError
import sqlparse
from sqlparse import tokens
from itertools import chain
import logging

parentddir = os.path.abspath('/cp-etl-apps/cp_elt_pipeline/automation_framework/main/python/utils')
sys.path.append(parentddir)
import sql_utils as sql_utils

class CP_Client(object):
    def __init__(self, cs , logSQLfile, identifiers , log_type , etl_batch_id ,  subjectarea , sqlScriptName ,
                 row_insert , row_update , row_delete , message , snowsql_startTime , snowsql_endTime,errno=None, emsg= None, sqlstate=None):
        self.cs = cs
        self.logSQLfile = logSQLfile
        self.identifiers = identifiers
        self.log_type = log_type
        self.etl_batch_id = etl_batch_id
        self.subjectarea = str(subjectarea)
        self.sqlScriptName = sqlScriptName
        self.row_insert = int(row_insert)
        self.row_update = int(row_update)
        self.row_delete = int(row_delete)
        self.message = message
        self.snowsql_startTime = snowsql_startTime
        self.snowsql_endTime  = snowsql_endTime
        self.errno = errno
        self.emsg = emsg
        self.sqlstate = sqlstate

    def insertActivity(self):
        logQuery = sql_utils.read_sql(self.logSQLfile)[0]
        logQuery = logQuery.format(**self.identifiers)
        log_literals = {'LOG_TYPE': self.log_type,
                        'BATCH_ID': self.etl_batch_id['ELT_BATCH_ID'],
                        'JOB_INPUT_ARGUMENT': str(self.subjectarea),
                        'SQL_SCRIPT_NAME': self.sqlScriptName,
                        'ERROR_CODE': self.errno,
                        'ERROR_MESSAGE': self.emsg,
                        'ERROR_STATE': self.sqlstate,
                        'ERROR_STACK_TRACE': None,
                        'ROWS_INSERTED': int(self.row_insert),
                        'ROWS_UPDATED': int(self.row_update),
                        'ROWS_DELETED': int(self.row_delete),
                        'SCRIPT_OUTPUT_MSG': self.message,
                        'EXEC_START_TIME': time.strftime('%Y-%m-%d %H:%M:%S',
                                                         time.localtime(self.snowsql_startTime)),
                        'EXEC_END_TIME': time.strftime('%Y-%m-%d %H:%M:%S',
                                                       time.localtime(self.snowsql_endTime))}
        self.cs.execute(logQuery, log_literals)