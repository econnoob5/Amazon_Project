import json
import os
import pprint
import requests

'''
This sample makes a call to the Bing Web Search API with a query and returns relevant web search.
Documentation: https://docs.microsoft.com/en-us/bing/search-apis/bing-web-search/overview
'''

# Add your Bing Search V7 subscription key and endpoint to your environment variables.
# environment_variables = os.environ
# os.environ['BING_SEARCH_V7_SUBSCRIPTION_KEY'] = '25abe3e1-b9e5-45ab-908d-885f0fba0e90'
#
# pprint.pprint(dict(environment_variables), width=1)
#
# endpoint = os.environ['https://management.azure.com/'] + "/bing/v7.0/search"

subscription_key = "e7b1c0928e514b3a8df2375b06572e37"
assert subscription_key
endpoint = "https://api.bing.microsoft.com/v7.0/search"

# Query term(s) to search for.
query = "nike"

# Construct a request
mkt = 'en-US'
params = {'q': query, "textDecorations": True, "textFormat": "HTML"}
headers = {'Ocp-Apim-Subscription-Key': subscription_key}

# Call the API
try:
    response = requests.get(endpoint, headers=headers, params=params)
    response.raise_for_status()
    search_results = response.json()

    # print("Headers:")
    # pprint.pprint(dict(response.headers), width=1)

    print("JSON Response:")
    pprint.pprint(dict(search_results), width=1)
except Exception as ex:
    raise ex
