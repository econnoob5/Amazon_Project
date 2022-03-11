import time
import re
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from time import sleep
import warnings
import os
import glob
from fuzzywuzzy import fuzz
from bs4 import BeautifulSoup
import numpy as np
import pathlib
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# setting directories
dir_load = r"../../Amazon Project - Data/Firms List/Firms lists from CIQ/Amazon project - US pub comp_all status2.xlsx"
firms_list = pd.read_excel(dir_load)
downloads_dir = r"C:\Users\MLaudi\Downloads"


# Defining function(s)
def latest_download():
    os.chdir(downloads_dir)
    files = sorted(os.listdir(os.getcwd()), key=os.path.getmtime)
    newest = files[-1]
    return newest


# Setting webdriver (OS)
# driver = webdriver.Chrome(
#     r"../Amazon Project - Code/chromedriver_mac")
# Setting webdriver (Win)
driver = webdriver.Chrome(
    r"../helpers/chromedriver.exe")

# setting credentials
username = "mlaudi@iese.edu"
password = "cap10126IESE!"

# We access Capital IQ and we log in with the username and password
driver.get("https://www.capitaliq.com")
WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.XPATH, '//*[@id="onetrust-accept-btn-handler"]'))).click()
driver.find_element_by_name("username").send_keys(username)
driver.find_element_by_name("password").send_keys(password)

WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="myLoginButton"]'))).click()


missing_info_final = pd.DataFrame()
testing = 0
number_of_downloads = 0

df_dwnld = pd.DataFrame()
df_nodwnld = pd.DataFrame()
df_nofirm = pd.DataFrame()
df_nofirmbutton = pd.DataFrame()
df_noprodbutton = pd.DataFrame()


for r in firms_list.itertuples():

    firm = r[1]
    search_box = 'cSearchBoxDisabled'
    no_firm_list_temp = []
    no_firm_button_list_temp = []
    no_products_button_list_temp = []

    try:
        element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, search_box)))
        element.click()
        element.send_keys(firm)
        driver.find_element_by_id('ciqSearchSearchButton').click()
    except:
        print('Missing firm: {}'.format(firm))
        no_firm_list_temp.append(firm)

    try:
        path1 = "//*[@id='SR0']/td[2]/div/span/a[normalize-space()='{}']".format(firm)
        WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.XPATH, path1))).click()
        # driver.find_element_by_xpath("//*[@id='SR0']/td[2]/div/span/a[normalize-space()='{}']".format(firm)).click()
    except:
        print('Missing firm button for: {}'.format(firm))
        no_firm_button_list_temp.append(firm)

    try:
        path2 = '//*[@id="ll_7_48_1338"][normalize-space()="Products"]'
        WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.XPATH, path2))).click()
        # driver.find_element_by_xpath('//*[@id="ll_7_48_1338"][normalize-space()="Products"]').click()
    except:
        print('Missing products button for: {}'.format(firm))
        no_products_button_list_temp.append(firm)

    try:
        path3 = '//*[@id="myExcelReport"]/div/img'
        WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.XPATH, path3))).click()
        # driver.find_element_by_xpath('//*[@id="myExcelReport"]/div/img').click()
        click_success = True
    except:
        print('Missing Excel products report for : {}'.format(firm))
        click_success = False

    # missing_info_series = pd.Series(missing_info_temp)
    # missing_info_final = missing_info_final.append(missing_info_series, ignore_index=True)

    if click_success:
        print('number of downloads = {}'.format(number_of_downloads))
        while True:
            count_files = 0
            for path in pathlib.Path("C:/Users/MLaudi/Downloads").iterdir():
                if path.is_file():
                    count_files += 1
            print('count_files is at {}'.format(count_files))
            if count_files == number_of_downloads:
                print('waiting')
                continue
            else:
                number_of_downloads += 1
                print('go')
                break

        firms_dwnld_list_temp = [firm]
        firms_dwnld_series_temp = pd.Series(firms_dwnld_list_temp)
        df_dwnld = df_dwnld.append(firms_dwnld_series_temp, ignore_index=True)

    else:
        firms_nodwnld_list_temp = [firm]
        firms_nodwnld_series_temp = pd.Series(firms_nodwnld_list_temp)
        df_nodwnld = df_nodwnld.append(firms_nodwnld_series_temp, ignore_index=True)

    no_firm_series_temp = pd.Series(no_firm_list_temp)
    df_nofirm = df_nofirm.append(no_firm_series_temp, ignore_index=True)

    no_firm_button_series_temp = pd.Series(no_firm_button_list_temp)
    df_nofirmbutton = df_nofirmbutton.append(no_firm_button_series_temp, ignore_index=True)

    no_products_button_series_temp = pd.Series(no_products_button_list_temp)
    df_noprodbutton = df_noprodbutton.append(no_products_button_series_temp, ignore_index=True)

    print('Done')

    # time.sleep(1)
    # firm_clean_name = re.sub(r"[-()\"#/@;:<>{}`+=~|.!?,]", "", firm)
    # latest_download_file = latest_download()
    # download_file_clean_name = re.sub(r"[-()\"#/@;:<>{}`+=~|.!?,]", "", latest_download_file)
    #
    # print(firm_clean_name, "|", download_file_clean_name)
    # ratio = fuzz.token_set_ratio(firm_clean_name, download_file_clean_name)
    # print(ratio)
    #
    # if ratio < 80:
    #     continue
    #
    # else:
    #     firms_list_temp = [firm]
    #     firms_series_temp = pd.Series(firms_list_temp)
    #     df_dwnld = df_dwnld.append(firms_series_temp, ignore_index=True)
    #     break

# try:
#     list_of_files = glob.glob('C:/Users/MLaudi/Downloads/*')  # * means all if need specific format then *.csv
#     latest_file = max(list_of_files, key=os.path.getctime)
#     latest_file_clean = latest_file[26:-12]
#     print(latest_file_clean)
#     ratio = fuzz.token_set_ratio(firm, latest_file_clean)
#     print(firm, "|", latest_file_clean, "|", ratio)
# except FileNotFoundError as err:
#     print(err)

# missing_info_final.columns = ['Missing firm', 'Missing firm button', 'Missing products button',
#                               'Missing Excel products report']


# missing_info_final_nonans = missing_info_final.drop(missing_info_final[(missing_info_final['Missing firm'] == 'nan') & (
#             missing_info_final['Missing firm button'] == 'nan') & (missing_info_final[
#                                                                        'Missing products button'] == 'nan') & (
#                                                                                    missing_info_final[
#                                                                                        'Missing Excel products report'] == 'nan')].index)
# missing_info_final.to_excel('../Amazon Project - Data/firms_products_link/test_ciq_link.xlsx')
# missing_info_final_nonans.to_excel('../Amazon Project - Data/firms_products_link/test_ciq_link_nonans.xlsx')

df_dwnld.to_excel('../Amazon Project - Data/firms_products_link/ciq_FirmsProd2.xlsx')
df_nodwnld.to_excel('../Amazon Project - Data/firms_products_link/ciq_FirmsProd_notdownloaded2.xlsx')
print('Process finished')

