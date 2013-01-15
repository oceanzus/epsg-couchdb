#!/usr/bin/env python

"""
@package epsg_utilities
@file epsg_utilities/epsg_utilities.py
@author James D. Case
@brief Helper class to lookup and load EPSG code data into CouchDB
"""

import csv
import urllib
import couchdb

def connect_couchdb(server_name):
    """
    Connects to and return a CouchDB instance
    """

    #Get an instance to the localhost CouchDB server
    server = couchdb.Server()

    # Attach to the epsg database
    db = server[server_name]

    return db

def init_epsg_couchdb():
    """
    Initializes an 'epsg' database on the local CouchDB instance
    WARNING! You will lose data
    """

    server = couchdb.Server()
    server.delete('epsg')
    server.create('epsg')


def get_wkt(epsg):
    """
    Get the OGC version of a WKT string for an EPSG code
    See http://spatialreference.org/ref/epsg/4326/ogcwkt/
    """

    f=urllib.urlopen("http://spatialreference.org/ref/epsg/{0}/ogcwkt/".format(epsg))
    if f.getcode() == 200:
        return f.read()
    else:
        return -1

def print_epsg_codes(csv_file_path):
    """
    Prints the EPSG codes from a valid EPSG CSV exported file
    """

    with open(csv_file_path, 'rb') as csv_file:
        gcs_reader = csv.reader(csv_file, delimiter=',', quotechar='#')
        for row in gcs_reader:
            print row[0]

def store_epsg(epsg_code, ogc_wkt):
    """
    Adds new document containing the OGC WKT into CouchDB keyed using the EPSG code
    """
    #Get an instance to the localhost CouchDB server
    epsg_db = connect_couchdb('epsg')
    try:
        epsg_db[epsg_code] = {'epsgcode': epsg_code, 'ogcwkt': ogc_wkt}
    except:
        print 'Failed to insert {0} into CouchDB.'.format(epsg_code)

def load_epsg_couchdb(csv_name, init_db=False):
    """
    Loads EPSG data from an epsg.org CSV file created using the GDAL utility
    into a local 'epsg' CouchDB database
    """

    # Delete and create the 'epsg' database
    if (init_db):
        init_epsg_couchdb()

    # Iterate over all the EPSG codes and place in CouchDB as unique documents
    with open(csv_name, 'rb') as csv_file:
        gcs_reader = csv.reader(csv_file, delimiter=',', quotechar='|')
        for row in gcs_reader:
            ogc_wkt = get_wkt(row[0])

            if (ogc_wkt <> -1):
                store_epsg(row[0], ogc_wkt)

def query_epsg(code):
    """
    Queries an EPSG code from the local 'epsg' CouchDB database
    """

    #Get an instance to the localhost CouchDB server
    epsg_db = connect_couchdb('epsg')
    if not isinstance(code, str):
        code = str(code)
    try:
        return epsg_db[code]
    except:
        return 'Not Found'
