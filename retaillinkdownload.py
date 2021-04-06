#!/usr/bin/env python3
__author__ = 'Anubha'

import sys
from selenium import webdriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
import time
import shutil
import fnmatch
import os , ast
import datetime
from datetime import datetime, timedelta
from selenium.webdriver.common.keys import Keys
from zipfile import ZipFile
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
parentddir = os.path.abspath('/cp-etl-apps/cp_elt_pipeline/automation_framework/main/python/module/')
sys.path.append(parentddir)
from CP_lib import CP_Client

parentddir = os.path.abspath('/cp-etl-apps/cp_elt_pipeline/automation_framework/main/python/utils')
sys.path.append(parentddir)
import config_utils as config_utils
from secret_manager import get_secret_passwd


class retailLinkDownload(object):

    def __init__(self, subjectarea_configPath, lc):
        self.lc = lc
        self.subjectarea_configPath = subjectarea_configPath

        self.literals = config_utils.readLiterals(self.subjectarea_configPath)
        # self.literalReports = config_utils.readReports(self.subjectarea_configPath)
        self.literalReports = config_utils.getConfigSection(self.subjectarea_configPath, "REPORT")
        self.report_filename = ast.literal_eval(config_utils.getConfig(subjectarea_configPath, "REPORT", "REPORT"))
        
       
        Main_Folder = self.subjectarea_configPath[:self.subjectarea_configPath.index("ini")]
        self.LOCAL_IN_DIR = Main_Folder + self.literals['LOCAL_IN_DIR'] + '/' + self.literals['YYYYMMDD'] 
        if not os.path.exists(self.LOCAL_IN_DIR):
            access_rights = 0o777
            os.makedirs(self.LOCAL_IN_DIR, mode=access_rights)

    # function to take care of downloading file
    def enable_download_headless(self, browser, download_dir):
        browser.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
        params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_dir}}
        browser.execute("send_command", params)

    # Initializing Chrome driver
    def init_driver(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("window-size=1400,1500")
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument('--verbose')
        options.add_argument('--disable-software-rasterizer')
        download_dir = os.path.abspath(self.LOCAL_IN_DIR)
        sys.path.append(download_dir)
        options.add_experimental_option("prefs", {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing_for_trusted_sources_enabled": False,
            "safebrowsing.enabled": False})
        driver = webdriver.Chrome(options=options)
        driver.wait = WebDriverWait(driver, 5)
        return driver

    # Walmart Retail Link Download
    def Walmart_Datapull(self):
        driver = self.init_driver()
        self.lc.logfile.info("--------- Initiating web driver ------------------")
        download_dir = os.path.abspath(self.LOCAL_IN_DIR)
        self.enable_download_headless(driver, download_dir)
        start_URL = self.literalReports['retail_url']
        User_Name = self.literalReports['retail_user_name']
        PWD = get_secret_passwd.get_secret_pass(self.literalReports['retail_pass_key'])
        #PWD='HI'
        report_filename = self.report_filename
        
        error_log_list = []
        try:
            driver.get(start_URL)
            time.sleep(5)
            driver.maximize_window()
            time.sleep(3)
            self.lc.logfile.info("----------- Logging in Walmart Retail -------------------")
            # Username
            Amazon_Username = driver.wait.until(EC.presence_of_element_located(
                (By.XPATH, "/html/body/div/div/div/div/div[2]/div[1]/div/div/form/span[1]/span/span/input")))
            Amazon_Username.click()
            Amazon_Username.send_keys(User_Name)
            # Password
            Pwd_Txtbox = driver.wait.until(EC.presence_of_element_located(
                (By.XPATH, "/html/body/div/div/div/div/div[2]/div[1]/div/div/form/span[2]/span/span/input")))
            Pwd_Txtbox.send_keys(PWD)
            # Login
            LogIn = driver.wait.until(EC.presence_of_element_located(
                (By.XPATH, "/html/body/div/div/div/div/div[2]/div[1]/div/div/form/button/span")))
            LogIn.click()
            self.lc.logfile.info("---------- Logged in Walmart Retail link -------------------------")
            # Status
            Status = driver.wait.until(EC.presence_of_element_located(
                (By.XPATH, "/html/body/form/table/tbody/tr/td/table/tbody/tr[1]/td[2]/a[2]")))
            Status.click()
            print("================Status======================")
            driver.switch_to.frame(driver.find_element_by_xpath("//*[@id='ifrContent']"))
            print("=========== switched to frame ifrContent ===========")
            time.sleep(20)
            # driver.switch_to.default_content()
            # print("=========== switched to Default Content ===========")
            # driver.switch_to.frame(driver.find_element_by_css_selector("//*[@name='JobTable']"))
            # driver.switch_to.frame(driver.find_element_by_xpath("//*[@id='JobTable']"))
            # driver.switch_to.frame(driver.find_element_by_css_selector("iframe[name='JobTable']"))
            # driver.switch_to.frame("JobTable")
            WebDriverWait(driver, 20).until(
                EC.frame_to_be_available_and_switch_to_it((By.XPATH, "//*[@id='JobTable']")))
            print("=========== switched to frame JobTable ===========")
            time.sleep(20)

            # HTML Job Table to dataframe
            html = driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            div = soup.select_one("table#myTable")
            table = pd.read_html(str(div))
            job_df = pd.DataFrame()
            job_df = table[0]
            job_df.columns = ['CheckBox', 'JobID', 'Status', 'Report_Name', 'RunTime', 'Size', 'Output']
            job_df['row_num'] = np.arange(len(job_df))
            self.lc.logfile.info("------------ Job Table Dataframe -----------------------------")
            self.lc.logfile.info(job_df)

            # Report file name from ini in pandas df
            File_download_flag = True
            print("reort_filename",report_filename)
            report_df = pd.DataFrame(report_filename)
            check_list_df = report_df
            check_list_df['Present'] = (check_list_df.merge(job_df,
                                                        how='outer',
                                                        left_on=['Report_Name'],
                                                        right_on=['Report_Name'],
                                                        indicator=True)
                                              .eval('_merge == "both"'))            
            report_df['Present'] = (report_df.merge(job_df,
                                                        how='inner',
                                                        left_on=['Report_Name'],
                                                        right_on=['Report_Name'],
                                                        indicator=True)
                                              .eval('_merge == "both"'))
            report_finaldf = pd.DataFrame()
            report_finaldf = report_df.loc[report_df['Present'] == False]
            print("report dataframe",report_finaldf)
            # If all the reports from the ini not scheduled in the Walmart Retail Link Portal throw error
            if not check_list_df.empty:
                report_missingList = check_list_df['Report_Name'].to_list()
                self.lc.logfile.info("Reports missing from Walmart Retail Link -> " +str(report_missingList))
                #File_download_flag = False
                #raise ValueError("Reports missing from Walmart Retail Link -> ", report_missingList)


            reportList = report_df['Report_Name'].to_list()
            self.lc.logfile.info("------------- Report List ----------------------")
            self.lc.logfile.info(reportList)
           
            ddate = self.literals['YYYY'] + '-' + self.literals['MM'] + '-' + self.literals['DD']
                        
            job_df = pd.merge(job_df,report_df,on=["Report_Name"],how="inner",indicator=True)
            print("modified job",job_df)
            c_maxes = job_df.groupby(['Report_Name']).RunTime.transform(max)
            job_df = job_df.loc[job_df.RunTime== c_maxes]
            print(job_df)
            for index, row in job_df.iterrows():
                print("iterrows:",index,row)
                try :
                    # Check whether filter conditions are satisfied
                    if str(row['Report_Name']) in reportList and str(row['Status']) in \
                            ["Done","Retrieved"] and ddate in str(row['RunTime']):
                        self.lc.logfile.info(" -------- Report Present in List ------------ ")
                        self.lc.logfile.info(row['Report_Name'])
                        time.sleep(10)
                        driver.switch_to.default_content()
                        self.lc.logfile.info("---------- Driver switched to default -----------")
                        time.sleep(20)
                        driver.switch_to.frame(driver.find_element_by_xpath("//*[@id='ifrContent']"))
                        self.lc.logfile.info("---------- Driver switched to ifrContent -----------")
                        time.sleep(10)
                        driver.switch_to.frame(driver.find_element_by_xpath("//*[@id='JobTable']"))
                        self.lc.logfile.info("---------- Driver switched to JobTable -----------")
                        time.sleep(10)
                        Report = driver.wait.until(EC.presence_of_element_located((By.XPATH, "//*[@id='myTable']/tbody/tr[" + str(int(row['row_num'] + 1)) + "]/td[1]/input")))
                        self.lc.logfile.info("---------- Driver switched to check Box -----------")
                        Report.click()
                        time.sleep(10)
                        Retrieve = driver.wait.until(
                            EC.presence_of_element_located(
                                (By.XPATH, "//*[@id='myTable']/tbody/tr[" + str(int(row['row_num'] + 1)) + "]/td[3]/span")))
                        self.lc.logfile.info("---------- Driver switched to Report -----------")
                        Retrieve.click()
                        time.sleep(100)
                        
                        # Rename file from local
                        filepath = download_dir
                       
                        filename = max([filepath + "/" + f for f in os.listdir(filepath) if f.endswith('.zip')], key=os.path.getctime)
                        reportPrefix = str(row['REPORT_FILENAME'])
                        filename_index = filename.rindex('/')
                        newfilepath = filename[:(filename_index + 1)] + reportPrefix + '_' + filename[(filename_index + 1):]
                        shutil.move(os.path.join(filepath, filename), newfilepath)
                    #else:
                        # If filter conditions are not satisfied raise exception
                        #raise ValueError("Reports not fulfilling the filter condition -> ", str(row['Report_Name']), str(row['Status']) ,
                        #                str(row['RunTime']) )
                        
                except Exception as err:
                    self.lc.logfile.info(err)
                    raise
            time.sleep(40)
            driver.quit()
        except TimeoutException:
            self.lc.logfile.info("Element not found")
            File_download_flag = False
            error_log_list.append("Exception details: " + str(sys.exc_info()) + "\n")
        return File_download_flag