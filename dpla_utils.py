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
