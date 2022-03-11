import pandas as pd
import numpy as np
import re
from bs4 import BeautifulSoup
import time
import json
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

""""""""""""""""""""""""""""""""""""""""""""" Setting directories """""""""""""""""""""""""""""""""""""""""""""""""""""
dir_load = r"../../Amazon Project - Data/Metadata/Metadata_small_brand_nan"
# dir_save has changed from the one below: the folder is is 'set_aside'. Change again if reusing the code
dir_save = r"../../Amazon Project - Data/Results/Amazon scraping"
# Setting webdriver
driver = webdriver.Chrome(
    r"/Amazon Project - Code/chromedriver.exe")

"""""""""""""""""""""""""""""""""""""""""""""" Defining functions """""""""""""""""""""""""""""""""""""""""""""""""""""


# The following two functions are used to parse the NOT GZIPPED json files and to open the as a Pandas DataFrame
def parse(path):
    with open(path) as jsonfile:
        for line in jsonfile:
            yield json.loads(line)


def getDF(path):
    i = 0
    df = {}
    for d in parse(path):
        df[i] = d
        i += 1
    return pd.DataFrame.from_dict(df, orient='index')


# function to generate URLs from search term
def get_url(search_term):
    # generate a URL from a search term
    template = 'https://www.amazon.com/s?k={}&ref=nb_sb_noss_2'
    search_term = re.sub('[^0-9a-zA-Z]+', '+', search_term)
    return template.format(search_term)


# function to get information in case there is a table in the product page
def get_info_from_table(soup_item):
    global collect_info  # as global just for debugging
    global manufacturer1  # as global just for debugging
    global manufacturer2  # as global just for debugging
    global manufacturer3  # as global just for debugging
    global manufacturer4  # as global just for debugging
    global df1  # as global just for debugging
    global df2  # as global just for debugging
    global df3  # as global just for debugging
    global df4  # as global just for debugging
    collect_info = []

    try:
        prod_table1 = soup_item.find('table', attrs={'id': 'productDetails_detailBullets_sections1'})
        str_table1 = str(prod_table1)
        dfs = pd.read_html(str_table1)
        df1 = dfs[0]
        df1 = df1[~df1[0].str.contains("By Manufacturer")]  # drop 'is discontinued by manufacturer'
        mask1 = df1[0].str.contains(r'Manufacturer', na=True)
        manufacturer1 = df1.loc[mask1, 1]
        collect_info.append(list(manufacturer1)[0])
    except:
        pass

    try:
        prod_table2 = soup_item.find('table', attrs={'id': 'productDetails_detailBullets_sections2'})
        str_table2 = str(prod_table2)
        dfs = pd.read_html(str_table2)
        df2 = dfs[0]
        df2 = df2[~df2[0].str.contains("By Manufacturer")]  # drop 'is discontinued by manufacturer'
        mask2 = df2[0].str.contains(r'Manufacturer', na=True)
        manufacturer2 = df2.loc[mask2, 1]
        collect_info.append(list(manufacturer2)[0])
    except:
        pass

    try:
        prod_table3 = soup_item.find('table', attrs={'id': 'productDetails_techSpec_section_1'})
        str_table3 = str(prod_table3)
        dfs = pd.read_html(str_table3)
        df3 = dfs[0]
        df3 = df3[~df3[0].str.contains("By Manufacturer")]  # drop 'is discontinued by manufacturer'
        mask3 = df3[0].str.contains(r'Manufacturer', na=True)
        manufacturer3 = df3.loc[mask3, 1]
        collect_info.append(list(manufacturer3)[0])
    except:
        pass

    try:
        prod_table4 = soup_item.find('table', attrs={'id': 'productDetails_techSpec_section_2'})
        str_table4 = str(prod_table4)
        dfs = pd.read_html(str_table4)
        df4 = dfs[0]
        df4 = df4[~df4[0].str.contains("By Manufacturer")]  # drop 'is discontinued by manufacturer'
        mask4 = df4[0].str.contains(r'Manufacturer', na=True)
        manufacturer4 = df4.loc[mask4, 1]
        collect_info.append(list(manufacturer4)[0])
    except:
        pass

    return collect_info


# function to extract the manufacturer: it uses 'get_info_from_table' function in case of a table and a div-id
# search key in case of no table
def extract_record(item):
    """ Extract records from one item """

    global results  # as global just for debugging
    global item_all_results  # as global just for debugging
    global manufacturer_from_table  # as global just for debugging
    url = get_url(item)
    driver.get(url)

    item_all_results = []  # stores all results from one item. It's the final output of the function.

    try:
        # loop through products 1-4 in the amazon page displaying the products after the title search
        for iterations in range(0, 4):

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            results_from_html = soup.find_all('div', {'data-component-type': 's-search-result'})

            item_page = results_from_html[iterations]
            atag = item_page.h2.a
            description = atag.text.strip()

            # wait for product page to open and click
            try:
                element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.LINK_TEXT, description)))
                element.click()
            except:
                print('help')

            # get current url after clicking (url of the specific product)
            soup2 = BeautifulSoup(driver.page_source, 'html.parser')

            # in case there is a table in the HTML code try 'get_info_from_table'. We need to repeat several times the
            # command because the information we need might be located in different tables from product to product.
            # Hence we look for manufacturer in all possible locations.
            # TODO (for mattia): The following try-except can probably be done with a function that uses macros. If have
            #  time make the code better by adding this feature.

            manufacturer_from_table = get_info_from_table(soup2)

            # it is possible that there is no table containing details. We need to find the information in another way
            try:
                go_to_details = soup2.find_all('div', {'id': 'detailBulletsWrapper_feature_div'})
                find_item = go_to_details[0]
                ultag = find_item.ul

                # get information in product details as list
                list_of_details = [li.text.replace('\n', '') for li in ultag.findAll('li')]
                # go in list of details and get manufacturer name. What appear in code is
                # 'Manufacturer:manufacturername'
                manufacturer5 = [m.replace('Manufacturer:', '') for m in list_of_details if
                                 m.startswith('Manufacturer:')]
                print(manufacturer5)
            except:
                manufacturer5 = 'nan'

            # TODO: results into a dict?
            # 'results' contains, in order: title input in amazon search bar, title of the product we click on,
            # manufacturer in case it is written in a table, manufacturer in case it is outside of a table
            results = {'title': item, 'product_caption': description,
                       'manufacturer_from_table': manufacturer_from_table, 'manufacturer_from_details': manufacturer5}
            # results = (item, description, manufacturer_from_table, manufacturer5)
            item_all_results.append(results)

            # return to results page
            driver.back()
    except:
        print('less than 8')

    return item_all_results


""""""""""""""""""""""""""""""""""""""""""""""""" Start scraping """""""""""""""""""""""""""""""""""""""""""""""""""""""
url = 'https://www.amazon.com'
driver.get(url)

file_counter = 0
df_all_manufacturers = pd.DataFrame()
for file in os.listdir(dir_load):
    file_counter += 1

    if file_counter < 4:
        continue

    elif file_counter == 4:
        filename = "/" + file
        df = getDF(dir_load + filename)
        df = df.drop_duplicates(subset='title')

        title_list = df['title'].to_list()

        df_manufacturer = pd.DataFrame(
            columns=['title', 'product_caption', 'manufacturer_from_table', 'manufacturer_from_details'])
        df_time = pd.DataFrame(columns=['index', 'seconds', 'cumulative_seconds'])
        index_title = 0
        start_time = time.time()
        for title in title_list:
            index_title += 1
            start_time2 = time.time()

            extract_manufacturer = extract_record(title)

            # 'dict_results' takes each tuple created in 'results' that is in the list 'extract_manufacturer' and
            # appends it to 'df_manufacturer'
            for dict_results in extract_manufacturer:
                manufacturer_series = pd.Series(dict_results)
                df_manufacturer = df_manufacturer.append(manufacturer_series, ignore_index=True)

            end_time = time.time()
            time_title = end_time - start_time2
            time_it_all = end_time - start_time

            percentage_done = (index_title / len(title_list)) * 100

            dict_append = {'index': index_title, 'seconds': time_title, 'cumulative_seconds': round(time_it_all)}
            df_time = df_time.append(dict_append, ignore_index=True)
            df_time['sma_10'] = df_time.iloc[:, 1].rolling(window=10).mean()

            if index_title > 10:
                last_sma10 = df_time.iloc[-1, 3]
                time_remaining = int(last_sma10) * (len(title_list) - index_title)
                print('Currently at: ' + str(
                    round(percentage_done,
                          4)) + '% | {} hours ({} seconds) since start of category | {} hours ({} seconds) remaining'.format(
                    round(time_it_all / 3600, 4), round(time_it_all, 4), round(time_remaining / 3600, 4),
                    round(time_remaining, 4)))

            else:
                print('Currently at: ' + str(
                    round(percentage_done,
                          4)) + '% | {} hours ({} seconds) since start of category'.format(
                    round(time_it_all / 3600, 4), round(time_it_all, 4)))

        # create column with category and add at end of df
        category_list = np.tile(file[5:-5], len(df_manufacturer))
        cat_series = pd.Series(category_list, name='main_cat')
        df_manufacturer = df_manufacturer.assign(main_cat=cat_series)

        df_manufacturer.to_csv(dir_save + filename[:-5] + '.csv')
        print('---------------------- Category Finished ----------------------')

    else:
        break