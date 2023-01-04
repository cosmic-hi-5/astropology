"""
Use the NED search service to retrieve information about objects.
"""

import requests

# Set the base URL for the NED search API
base_url = "https://ned.ipac.caltech.edu/byname?objname={}"

# Set the name of the object to search for
object_name = "WISEA+J000036.42-055657.8"

# Format the base URL with the object name
url = base_url.format(object_name)
print(url)
# Set the parameters for the GET request
params = {
    "hconst": 67.8,
    "omegam": 0.308,
    "omegav": 0.692,
    "wmap": 4,
    "corr_z": 1
}

# Send the GET request to the NED search API
response = requests.get(url, params=params, timeout=10)

# Print the response from the server
# print(response.text)
