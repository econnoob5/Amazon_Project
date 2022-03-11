"""
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
Title: Scrapping Capital IQ 'public ownership' data.
Description: This code will be used each time you want to scrap public ownership data from Capital IQ. However, this code might be of help when constructing a
             different one that scraps different data, either from Capital IQ or any other source.
             This code is also parameter-based, so before everything you will have to set the parameters according to how you want to run the code.
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
"""

import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from time import sleep
import os  # This library will help us scan through the files
import pathlib

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "\\"  # this gets the project root path

# firms_list_content_root_path = "Amazon Project - Data/Firms lists from CIQ/Company_list_5652.xls"
firms_list = pd.read_excel(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Firms lists from CIQ\Company_list_5652.xlsx")

start_time = time.time()
"""
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
PARAMETERS
    1. COMPUTER: Whether we will be using Edu or Luca's computer. This should be adapted for next users.
    2. OPEN_DRIVER: Whether to start the webdriver or not. If you run the code for a second time (after having already opened the webdriver once), then Selenium
       will open a second web driver. The problem with this is that if you have already logged into Capital IQ in the other webdriver, then after opening the
       second webdriver, the first one will be closed, so you would not be able to log out properly of Capital IQ from the first webdriver. Then, if you
       try to log into Capital IQ in the second driver, it will fail because Capital IQ will detect an outgoing session in another webdriver. This should be 
       avoided at all costs because Mireia Gine gets a notification when this happens. 
    3. LOGIN: Set to True if you want to log into Capital IQ, or set to False if the webdriver has already been opened and you have already logged in. 
    4. START: (int) First index to be iterated from the companies list.
    5. I_OPEN: (int) Number of iterations
    6. ALL_ROWS: Iterate through all companies from the list
    7. SECURITIES_TRACK: Leave always as True so it computes the dataframe with the id of each security (which is gotten from the HTML tags)
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
"""
# DEFINING PARAMETERS
open_driver = True
login = True
i_open = len(firms_list['Company Name'])
open_companies_list = True
click = True
close_driver = True
all_rows = True
securities_track = True

# We will save the downloaded files in here:
list_of_companies_path = "Amazon Project - Data/Firms lists from CIQ/Company_list_5652.xls"  # TODO(mattia): why defining _output and _track? Python does not seem to like it very much
# securities_track_path = ROOT_DIR + "_track\securities_track.csv"
companies_track_path = ROOT_DIR + " Amazon Project - Data\_track\companies_track.csv"

# WEBDRIVER
if open_driver:
    print(" Executing webdriver")

    options = webdriver.ChromeOptions()  # TODO(mattia): explanation of next 7 lines?
    options.add_argument("--start-maximized")
    prefs = {"profile.default_content_settings.popups": 0,
             "download.default_directory": ROOT_DIR + "Amazon Project - Data\_output",  # Important, ending in slash
             "directory_upgrade": True}
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome("chromedriver.exe", options=options)

    driver.set_page_load_timeout(30)

""" Logging into Capital IQ. Remember: It should be done only once after executing the webdriver. """
if login:
    print(" Logging in Capital IQ")
    driver.get("https://www.capitaliq.com")
    # driver.find_element_by_name("username").send_keys("gine@wharton.upenn.edu")
    # driver.find_element_by_name("password").send_keys("vis8NOVciq1")

    # driver.find_element_by_name("username").send_keys("eduard.seyde@brattle.com")
    # driver.find_element_by_name("password").send_keys("Lm301910$")

    # remember to input correct user and pass before launching the code!
    driver.find_element_by_name("username").send_keys("eseyde@iese.edu")
    driver.find_element_by_name("password").send_keys("WhiteSnow14%")

    driver.find_element_by_name("chkPersistentLogin").click()
    driver.find_element_by_name("myLoginButton").click()

    sleep(5)  # Sometimes there are problems if right after logging in we start scraping data.

if open_companies_list:
    print(" Importing companies list")
    companies_list = pd.read_csv(list_of_companies_path)
    # companies_list["company_id"] = companies_list["company_id"].apply(lambda x: x.replace("IQ","")).astype(int)

# try:
#     securities_summary = pd.read_csv(securities_track_path)
#     print(" Note: 'securities_track.csv' already exists")
# except:
#     securities_summary = pd.DataFrame([], columns=["company_name","company_id","security_name","security_id"])
#     securities_summary.to_csv(securities_track_path, index=False)

try:
    companies_track = pd.read_csv(companies_track_path)
    print(" Note: 'companies_track.csv' already exists")
except:
    companies_track = pd.DataFrame([], columns=["company_id"])
    companies_track.to_csv(companies_track_path, index=False)

# securities_summary_x = []  # New dataframe securities to be append in the previous one
# log_notfound = []  # We will keep a list of those links not found

"""
------------------------------------------------------------------------------------------------------------------------
START SCRAPPING (LOOP THROUGH EACH COMPANY)
------------------------------------------------------------------------------------------------------------------------
"""
link = "https://www.capitaliq.com/CIQDotNet/Ownership/CompanyHistory.aspx?companyId="
for row in range(0, i_open):
    print("\n Scrapping company number " + str(row))
    company_id = companies_list.iloc[row, 1]

    # We will skip this company if we have already downloaded it
    companies_track = pd.read_csv(companies_track_path)

    if len(companies_track) != 0:
        if company_id in companies_track.company_id.unique().tolist():
            print(" Company already downloaded in -> skipping it")
            continue

    pd.DataFrame([company_id]).to_csv(companies_track_path,
                                      mode="a",  # Important, this appends into the existing csv file
                                      index=False,
                                      header=False)
    try:
        driver.get(link + str(company_id))
    except:
        log_notfound.append(link)
        continue

    company_name = driver.find_element_by_id("cph_PageHeaderLabel").get_attribute("innerText")
    company_name = company_name[:company_name.find("(") - 1]

    try:
        if click:
            driver.find_element_by_id("hNavHistory_myDisplaySection_leftleftNavButton").click()
        else:
            pass
    except:
        raise ValueError("Unable to click the 'expand' public ownershp history.")

    try:
        # This access the dropdown menu for selecting security type
        obj = Select(driver.find_element_by_name("ctl02$customizeView$ddlAgg$_securityView$DDL"))
    except:
        raise ValueError("Unable to access dropdown menu for selecting security type.")

    for i in range(0, len(obj.options)):
        obj = Select(driver.find_element_by_name("ctl02$customizeView$ddlAgg$_securityView$DDL"))
        obj.select_by_index(i)
        security_id = obj.first_selected_option.get_attribute("value")
        if security_id == "-1":
            continue
        security_name = obj.first_selected_option.get_attribute("innerText")

        if "Depositary Receipt" not in security_name:  # We will not download data for depositary receipts
            if click:
                # Downloading files
                driver.find_element_by_name("ctl02$customizeView$goButton").click()
                driver.find_element_by_id("excelReport").click()
                sleep(2)

            # Appending data into "securities" data summary
            if securities_track and click:
                securities_row = {"company_name": company_name,
                                  "company_id": company_id,
                                  "security_name": security_name,
                                  "security_id": security_id}
                securities_summary_x.append(securities_row)

                pd.DataFrame([securities_row]).to_csv(securities_track_path,
                                                      mode="a",
                                                      header=False,
                                                      index=False)

            print(" - Security:", security_name)

if securities_track and click:
    securities_summary_x = pd.DataFrame(securities_summary_x,
                                        columns=["company_name", "company_id", "security_name", "security_id"])
    securities_summary = pd.concat([securities_summary, securities_summary_x])
    securities_summary.drop_duplicates(inplace=True)  # Drop duplicates in case we have ran twice the same row
    # securities_summary.to_csv(script_path+"_track\securities_track.csv",index=False)

if close_driver:
    driver.close()

print('It took', round((time.time() - start_time) / 60, 1), 'minutes.')
