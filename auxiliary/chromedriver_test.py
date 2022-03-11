import time

from selenium import webdriver

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
browser = webdriver.Chrome("/data/users/mlaudi/Amazon/Amazon Project - Code/chromedriver_linux", options=chrome_options)

browser.get(r'https://www.google.com')
print( "Page title was '{}'".format(browser.title))

browser.quit()
