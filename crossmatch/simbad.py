"""This module contains a function for querying the SIMBAD database."""
import urllib.request
import urllib.parse

# Set the base URL for the SIMBAD search service
base_url = "http://simbad.u-strasbg.fr/simbad/sim-script"

# Set the parameters for the search query
params = {
    "script": "output",
    "output.format": "ASCII",
    "object.bibcode": "all",
    "obj.coo": "on",
    "obj.bib": "on",
    "obj.fields": "on",
    "obj.opt": "on",
    "obj.simbadonly": "on",
    "obj.urls": "on",
    "obj.ref": "on",
    "obj.notes": "on",
    "obj.types": "on",
    "obj.id": "on",
    "obj.status": "on",
    "obj.cdsref": "on",
    "obj.alias": "on",
    "obj.lastmodif": "on",
    "obj.lastobj": "on",
    "obj.lastnote": "on",
    "obj.lasturl": "on",
}

# Set the name or designation of the celestial object to search for
object_name = "WISEA J000036.42-055657.8"

# Encode the search parameters and append them to the base URL
query_url = base_url + "?" + urllib.parse.urlencode(params)
print(query_url)

# Send the search request to the SIMBAD server and retrieve the results
with urllib.request.urlopen(query_url) as response:
    search_results = response.read()

# Print the search results
print(search_results)
