"""
Functionality for querying the Chandra X-ray Observatory
"""

import urllib.request
import urllib.parse

# Set the base URL for the Chandra search service
base_url = "https://cda.harvard.edu/chaser/query"

# Set the parameters for the search query
params = {
    # Set the database to search in (optional)
    "db_key": "CDA",
    # Set the name or designation of the celestial object to search for
    "object": "WISEA J000036.42-055657.8",
    # Set the format of the search results (can also be "XML" or "HTML")
    "outputformat": "CSV",
}

# Encode the search parameters and append them to the base URL
query_url = base_url + "?" + urllib.parse.urlencode(params)

print(query_url)
# Send the search request to the Chandra server and retrieve the results
with urllib.request.urlopen(query_url) as response:
    search_results = response.read()

# Print the search results
print(search_results)
