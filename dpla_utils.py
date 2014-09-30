from __future__ import print_function

"""
:Author: Madhura Parikh (madhuraparikh@gmail.com)

This module contains basic utility functions for querying dp.la.
Also supports interactions with a mongodb database
"""

import warnings
from collections import defaultdict

import requests
import pymongo

# define some common variables that can be used globally
collections_url = 'http://api.dp.la/v2/collections'
items_url = 'http://api.dp.la/v2/items'

hub_largest_collections = {u'ARTstor': u'3f0e282f7fed21d7790fce877faf11d7', 
                   u'The New York Public Library': u'a9c225b7b9801ca4a84c63d1ddc64fd2', 
                   u'David Rumsey': u'ed02f0fad03b7c6ed1000cceb371b8a1', 
                   u'Digital Commonwealth': u'460c76299e1b0a46afea352b1ab8f556', 
                   u'Minnesota Digital Library': u'49b09ce719c5184f166920a1a7c1e8cd', 
                   u'Biodiversity Heritage Library': u'24087df2c58f149a8167a9589cffd830', 
                   u'National Archives and Records Administration': u'df8bd84b8c0e542746b5d1ed57ab5fee', 
                   u'Smithsonian Institution': u'3dc8442b62578dd60c322e2599550b12', 
                   u'Mountain West Digital Library': u'c13cf266db030a231b3d7b0b881dd0d5', 
                   u'University of Illinois at Urbana-Champaign': u'1cfd29b7fcf303d0becf1145a0d2a2be', 
                   u'Digital Library of Georgia': u'a72045095d4a687a170a4f300d8e0637', 
                   u'J. Paul Getty Trust': u'b7b9d0195e635c2536a9b06579a1d990', 
                   u'The Portal to Texas History': u'eb100f9e44e0369adce25e675b355fc4', 
                   u'Harvard Library': u'4b7a59eb67cfb3c2dc0df20c7ba08416', 
                   u'University of Virginia Library': u'a0cd618386dd6e6b3c548d7ba54b7663', 
                   u'Internet Archive': u'190b7921c442008cb8e538f61cdab6cb', 
                   u'North Carolina Digital Heritage Center': u'e2c11e2ec8b4c3e5ee39494057a56751'}

def send_request(url, payload):
    """
    This function sends a HTTP get request to the given url and returns the corresponding
    response parsed as json.

    Parameters
    ----------
    url : str
        The url of the remote server.
    payload : dict
        The dictionary representing the params of the HTTP request

    Returns
    -------
    json parse of `Requests.response` object.

    Examples
    --------
    >>> payload = { 'api_key' : 000000 }
    >>> dpla_json = send_request(collections_url, payload)
    """
    try:
        response = requests.get(url, params=payload)
        return response.json()
    except requests.exceptions.RequestException:
        raise Exception ("HTTP request failed.")


def dpla_fetch(api_key, count, search_type='items', **kwargs):
    """
    Fetches the specified number of resources with simple search by default, and
    supports more selective queries by optional arguments.

    Parameters
    ----------
    api_key : str
        The api key generated from dp.la.
    count : int
        The number of resources to be fetched.
    search_type : str, optional
        the type of resource to fetch. Should be either 'items' or 'collections'.
        Defaults to 'items'.

    Returns
    -------
    dpla_results : dict
        The collections from dpla in JSON format.

    Examples
    --------
    >>> api_key = '000000'

    >>> # fetches the first 1200 items
    >>> items = dpla_fetch(api_key, 1200)

    >>> # fetches all items that mention kitten somewhere
    >>> items = dpla_fetch(api_key, q='kitten*')

    >>> # for nested fields (that have periods in them) do this
    >>> conditions = { 'sourceResource.collection.title' : 'Smith' }
    >>> # this will return all items from the colletion titled Smith
    >>> items = dpla_fetch(api_key, **conditions)
    """

    # first of all set the page_size
    page_size = 500
    final_page_size = count % 500
    num_pages = count / 500

    # now construct the payload dict
    payload = dict()
    payload['api_key'] = api_key
    for key in kwargs:
        payload[key] = kwargs[key]

    # fetch the query results
    if search_type == 'collections':
        url = collections_url
    else:
        url = items_url
    json_dics = []
    page_count = 0
    for i in range(1, num_pages + 1, 1):
        payload['page_size'] = 500
        payload['page'] = i
        response = send_request(url, payload)
        json_dics.append(response)
        page_count = i
    if final_page_size:
        payload['page'] = page_count + 1
        payload['page_size'] = final_page_size
        response = send_request(url, payload)
        json_dics.append(response)
    # combine all the results
    dpla_results = []
    for dic in json_dics:
        docs = dic.get('docs', [])
        for doc in docs:
            dpla_results.append(doc)

    return dpla_results


def connect_to_mongodb():
    """ Returns connection to a running mongodb server """
    try:
        conn=pymongo.MongoClient()
        print("Connected successfully")
        return conn
    except pymongo.errors.ConnectionFailure as e:
       print("Could not connect to MongoDB", e)
