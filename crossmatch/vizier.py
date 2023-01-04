"""
This module contains a function for querying the VizieR catalog
search service.
"""
import urllib.request
import urllib.parse

# Set the base URL for the VizieR search service
base_url = "http://vizier.u-strasbg.fr/viz-bin/votable/-A"

# Set the parameters for the search query
params = {
    # Set the catalog or survey to search in (optional)
    "source": "J/AJ/154/51",
    # Include all columns in the search results
    "-out.all": "",
    # Set the format of the search results (can also be "votable" or "tsv")
    "-mime": "csv",
    # Set the coordinate system for celestial coordinates (optional)
    "-c.eq": "J2000",
    # Set the search radius for celestial coordinates (optional)
    "-c.r": "0.2",
    # Include celestial coordinates in the search results (optional)
    "-out.add": "RAJ2000,_DEJ2000",
    # Set the name or designation of the celestial object to search for
    "object": "WISEA J000036.42-055657.8",
}

# Encode the search parameters and append them to the base URL
query_url = base_url + "?" + urllib.parse.urlencode(params)

# Send the search request to the VizieR server and retrieve the results
with urllib.request.urlopen(query_url) as response:
    search_results = response.read()

# Print the search results
print(search_results)
print(query_url)
