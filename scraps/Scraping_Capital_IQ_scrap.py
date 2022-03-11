"""
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
Title: Scrapping Capital IQ 'public ownership' data.
Description: This code will be used each time you want to scrap public ownership data from Capital IQ. However, this code might be of help when constructing a
             different one that scraps different data, either from Capital IQ or any other source.
             This code is also parameter-based, so before everything you will have to set the parameters according to how you want to run the code.
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
"""
import selenium as sl
import time
import pandas as pd
from bs4 import BeautifulSoup
import requests as rq
from selenium import webdriver
from selenium.webdriver.support.ui import Select
import webbrowser
from time import sleep
import os

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

RECOMMENDATION: I would first run until line 62, so that I only open the webdriver. Then, I will go manually to the webdriver and change the download folder settings
so that all files downloaded from that webdriver go exactly to the selected folder that I want (data source in Capital IQ dropbox), that way we avoid unwanted 
files when constructing the dataset that would cause the code to crash. Once I have done this, I can re-run the code but setting 'open_driver = False' and 
'login=True'. If you haven't done any scrapping yet, then set 'start=0' and 'i_open' for as many iterations as you want. Then you can re-run it again for more
iterations (BUT YOU HAVE TO REMEMBER WHERE DID THE SCRAPPING FINISHED THE LAST TIME!!). An easier option is to check that it works well by running a few iterations, 
and if it works well delete all the downloaded files (so we don't have duplicated files) and re-run it again setting 'all_rows = True'
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
"""
computer = "Edu"
open_driver = True
login = True
start = 0
i_open = 10
open_companies_list = True
click = False
close_driver = False
all_rows = True
securities_track = True

if computer == "Luca":
    initial_path = r"C:\Users\LRoncella\Dropbox " # This last space is important
else:
    initial_path = r"D:\Dropbox " # This last space is important

""" Opening webdriver. Remember to open it only if it is not already opened! """
if open_driver == True:
    print("· Executing webdriver")
    driver = webdriver.Chrome(initial_path+r"(IESE)\EDU&LUCA\Capital IQ\Programs\Scrapping\chromedriver.exe")
    # In case Chrome driver fails:
    # driver = webdriver.Firefox(executable_path=r"D:\Dropbox (IESE)\Common Ownership Around the World\2. Data\Capital IQ\2 - Codes\2 - Scrapping\geckodriver.exe")
    driver.set_page_load_timeout(30)

""" Logging into Capital IQ. Remember: It should be done only once after executing the webdriver. """
if login == True:
    print("· Logging into Capital IQ")
    driver.get("https://www.capitaliq.com")
    driver.find_element_by_name("username").send_keys("gine@wharton.upenn.edu") #TODO: change user
    driver.find_element_by_name("password").send_keys("vis8NOVciq1") #TODO: change password
    driver.find_element_by_name("chkPersistentLogin").click()
    driver.find_element_by_name("myLoginButton").click()

""" 
You will probably not have access to this list, don't worry. I have duplicated this list and it is available at:
IESE - Common Ownership Around the World - Data - Capital IQ - Data - Other Data
"""
if open_companies_list == True: #TODO: substitute with our list
    print("· Importing companies list")
    companies_list = pd.read_excel(initial_path+r"(IESE)\EDU&LUCA\Capital IQ\Data\0  - Summary\Rest_of_the_World_Links.xlsx")

"""
--------------------------------------------------------------------------------------------------------------------------------------------
    As described before, everytime we scrap data from a given company, we get its securities' ids from the HTML tags. This IDs are kept in the 
    following dataframe. Since this code can be run multiple times, in different iterations, at the end of each code the dataframe will be saved
    in the following path. So before starting again, it has to be imported again. I have called this dataframe All.securities_2.csv to avoid it
    been overwritten, so the next time you run it you can call it like that. The original one (called ALL.securities) is being used in the
    0 - Master Code - All in One code.
--------------------------------------------------------------------------------------------------------------------------------------------
"""
try:
    securities_summary = pd.read_csv(r"D:\Dropbox (IESE)\Common Ownership Around the World\2. Data\Capital IQ\1 - Data\4 - Data Source\Other Files\ALL.securities_2.csv")
except:
    if start == 0:
        securities_summary = pd.DataFrame()
    else:
        raise ValueError("Securities.csv file has not been found, while start is not 0")
if start == 0:
    securities_summary = pd.DataFrame()

securities_summary_x = [] # New dataframe securities to be append in the previous one
log_notfound = [] # We will keep a list of those links not found

if all_rows == True:
    lenght = len(companies_list)
    i_open = lenght-start

"""
--------------------------------------------------------------------------------------------------------------------------------------------
START SCRAPPING (LOOP THROUGH EACH COMPANY)
--------------------------------------------------------------------------------------------------------------------------------------------
"""
for row in range(start, start+i_open):
    link = companies_list.iloc[row,4]
    print("\nScrapping company number "+str(row)+" --> "+link)
    #sleep(1)
    try:
        driver.get(link)
    except:
        log_notfound.append(link)
        continue

    try:
        company_name = driver.find_element_by_id("cph_PageHeaderLabel").get_attribute("innerText")[:-27]
    except:
        print("Unable to get company's name.")
        company_name = "ERROR"

    try:
        if click == True:
            driver.find_element_by_link_text("<<").click()
    except:
        pass

    try:
        obj = Select(driver.find_element_by_name("ctl02$customizeView$ddlAgg$_securityView$DDL"))
    except:
        continue # If the company does not have "public history ownership" proceed to next company

    try:
        if obj.select_by_value("-1") != False:
            for i in range(1,len(obj.options)):
                obj = Select(driver.find_element_by_name("ctl02$customizeView$ddlAgg$_securityView$DDL"))
                obj.select_by_index(i)
                security_id = obj.first_selected_option.get_attribute("value")
                security_name = obj.first_selected_option.get_attribute("innerText")
                if click == True:
                    driver.find_element_by_name("ctl02$customizeView$goButton").click()
                    driver.find_element_by_id("excelReport").click()

                # Appending data into "securities" data summary
                if securities_track == True:
                    securities_row = {}
                    securities_row["link"] = link
                    securities_row["company_name"] = company_name
                    securities_row["security_id"] = security_id
                    securities_row["security_name"] = security_name
                    securities_summary_x.append(securities_row)
                print("· Security:",security_name)

    except:
        for i in range(0,len(obj.options)):
            obj = Select(driver.find_element_by_name("ctl02$customizeView$ddlAgg$_securityView$DDL"))
            obj.select_by_index(i)
            security_id = obj.first_selected_option.get_attribute("value")
            security_name = obj.first_selected_option.get_attribute("innerText")
            if click == True:
                driver.find_element_by_name("ctl02$customizeView$goButton").click()
                driver.find_element_by_id("excelReport").click()

            # Appending data into "securities" data summary
            if securities_track == True:
                securities_row = {}
                securities_row["link"] = link
                securities_row["company_name"] = company_name
                securities_row["security_id"] = security_id
                securities_row["security_name"] = security_name
                securities_summary_x.append(securities_row)
            print("· Security:",security_name)

start = start + i_open
if securities_track == True:
    securities_summary_x = pd.DataFrame(securities_summary_x,columns=["link","company_name","security_name","security_id"])
    securities_summary = pd.concat([securities_summary,securities_summary_x])
    securities_summary.drop_duplicates(inplace=True) # Drop duplicates in case we have ran twice the same row
    securities_summary.to_csv(r"D:\Dropbox (IESE)\Common Ownership Around the World\2. Data\Capital IQ\1 - Data\4 - Data Source\Other Files\ALL.securities_2.csv"),index=False)

    if close_driver == True:
        pass
    #driver.close()

    print('It took', round((time.time()-start_time)/60, 1), 'minutes.')
    del start_time