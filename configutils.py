#!/usr/bin/env python3
__author__ = 'Anubha'

import datetime
import collections
from configparser import ConfigParser


def getConfig( inifile, section, name):
    # Function to read ini file
    # Args - Ini file location , Section of ini file, key in section
    # Returns - Ini file value
    config = ConfigParser()
    config.read(inifile)
    return config.get(section, name)

def getConfigSection(inifile, section):
    parser = ConfigParser()
    parser.read(inifile)
    return dict(parser.items(section))


def checkConfigSection(inifile):
    parser = ConfigParser()
    parser.read(inifile)
    return parser.sections()

def readLiterals(subjectarea_configPath):
    literals = getConfigSection(subjectarea_configPath, "PARAMETER")
    if literals['yyyy'] == "" or literals['mm'] == "" or literals['dd'] == "":
        literals['yyyy'] = datetime.date.today().strftime("%Y")
        literals['mm'] = datetime.date.today().strftime("%m")
        literals['dd'] = datetime.date.today().strftime("%d")

    literals = {k.upper(): v for k, v in literals.items()}
    literals['YYYYMMDD'] = literals['YYYY'] + literals['MM'] + literals['DD']

    def default_factory():
        return 'N'

    literals = collections.defaultdict(default_factory, literals)
    print(literals)
    return literals

def readCommon(common_ini_filepath):
    aws_details = getConfigSection(common_ini_filepath, "AWS_DETAILS")
    return aws_details
