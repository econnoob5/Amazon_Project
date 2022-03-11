from bs4 import BeautifulSoup
import pandas as pd

directory = r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Patents and trademarks\asb19550103-20201231-01.xml"
data = pd.read_xml(directory)


# Reading the data inside the xml
# file to a variable under the name
# data
# directory = r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Patents and trademarks\asb19550103-20201231-01.xml"
# with open(directory, 'r') as f:
#     data = f.read()

# # Passing the stored data inside
# # the beautifulsoup parser, storing
# # the returned object
# Bs_data = BeautifulSoup(data, "xml")
#
# # Finding all instances of tag
# # `unique`
# b_unique = Bs_data.find_all('unique')
#
# print(b_unique)
#
# # Using find() to extract attributes
# # of the first instance of the tag
# b_name = Bs_data.find('child', {'name': 'Frank'})
#
# print(b_name)
#
# # Extracting the data stored in a
# # specific attribute of the
# # `child` tag
# value = b_name.get('test')

# print(value)
