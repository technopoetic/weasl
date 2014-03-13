#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys, getopt
import os
import datetime
import urllib2
import csv
from datetime import timedelta
import xml.etree.ElementTree as ET
from itertools import tee, islice, chain, izip
import ConfigParser

Config = ConfigParser.ConfigParser()
Config.read("weasl.cfg")

# This funtion allows me to access the next and previous elements while iterating through a list.
def previous_and_next(some_iterable):
    prevs, items, nexts = tee(some_iterable, 3)
    prevs = chain([None], prevs)
    nexts = chain(islice(nexts, 1, None), [None])
    return izip(prevs, items, nexts)

# Gets all cores for the solr installation by parsing the list of cores in the admin interface
def get_cores_list():
    admin_url = Config.get("Solr server", "master_host") + "/solr/admin/cores?action=STATUS"
    cores_list = []
    tree = ET.parse(urllib2.urlopen(admin_url))
    root = tree.getroot()
    status =  root.find(".//lst[@name='status']")
    for child in status:
        cores_list.append(child.get('name'))
    return cores_list

# Given a start and end date, returns a list of all the dates in between, inclusive.
def date_range(start, end):
    r = (end+datetime.timedelta(days=1)-start).days
    return [start+datetime.timedelta(days=i) for i in range(r)]

# Given a list of timestamps, print a list of the number of documents added per timestamp.
# TODO: Make this more general.  Maybe it should take a query parameter as well, and it should return something.
def get_docs_all_cores(timestamps):
    cores = get_cores_list()
    for core in cores:
        print "\n" + core 
        for previous, item, next in previous_and_next(timestamps):
            if(next):
                query = '/solr/{0}/select/?q=pubsys_asset_creation_dt%3A%5B{1}+TO+{2}%5D&start=0&rows=1'.format(core, item.strftime("%s"), next.strftime("%s"))
                url_string = Config.get("Solr server", "master_host") + query
                tree = ET.parse(urllib2.urlopen(url_string))
                rootElem = tree.getroot().find('result')
                print item.strftime("%Y-%m-%d") + ": " + rootElem.attrib.get('numFound')

# Same as get_docs_all_cores, but output the results as a csv file.
def get_docs_all_csv(timestamps):
    cores = get_cores_list()
    rows = []
    header_row = list(cores)
    header_row.insert(0,"Date")
    rows.append(header_row)
    for previous, item, next in previous_and_next(timestamps):
        data_row = []
        if(next):
            data_row.append(item.strftime("%Y-%m-%d"))
            for core in cores:
                url_string = Config.get("Solr server", "master_host") + '/solr/{0}/select/?q=pubsys_asset_creation_dt%3A%5B{1}+TO+{2}%5D&start=0&rows=1'.format(core, item.strftime("%s"), next.strftime("%s"))
                tree = ET.parse(urllib2.urlopen(url_string))
                rootElem = tree.getroot().find('result')
                data_row.append(rootElem.attrib.get('numFound'))
        rows.append(data_row)
    with open('solr_docs.csv', 'wb') as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    print "Done!"

# Given a core name, and a list of timestamps, prints a count of all docs added to that core per day.
# TODO: Combine this with get_docs_all_cores.  If single core parameter is passed, then only query that core, else query all cores.
def get_docs_single_core(core, timestamps):
    for previous, item, next in previous_and_next(timestamps):
        if(next):
            url_string = Config.get("Solr server", "master_host") + '/solr/{0}/select/?q=pubsys_asset_creation_dt%3A%5B{1}+TO+{2}%5D&start=0&rows=1'.format(core, item.strftime("%s"), next.strftime("%s"))
            tree = ET.parse(urllib2.urlopen(url_string))
            rootElem = tree.getroot().find('result')
            print item.strftime("%Y-%m-%d") + ": " + rootElem.attrib.get('numFound')

# Given a core, a start timestamp, and an end timestamp, get a list of the actual docs (The actual XML document objects).
def get_docs_(core, start, end):
    url_string = Config.get("Solr server", "master_host") + '/solr/{0}/select/?q=pubsys_asset_creation_dt%3A%5B{1}+TO+{2}%5D&start=0&rows=1'.format(core, start.strftime("%s"), end.strftime("%s"))
    tree = ET.parse(urllib2.urlopen(url_string))
    rootElem = tree.getroot().find('result')
    doclist = rootElem.findall('doc')
    for doc in doclist:
        print doc


# Execute a query across all cores.
def query_multi_core(query):
    cores = get_cores_list()
    results = []
    numResults = 0
    for core in cores:
        url_string = Config.get("Solr server", "master_host") + '/solr/{0}/select/?q={1}'.format(core, query)
        try:
            tree = ET.parse(urllib2.urlopen(url_string))
            rootElem = tree.getroot().find('result')
            print "\n" + core + ": " + url_string
            print  "Results: " + rootElem.attrib.get('numFound')
            numResults += int(rootElem.attrib.get('numFound'))
        except urllib2.HTTPError:
            print "Error connecting to core: {0}".format(core)
    print "Total Results across all cores: " + str(numResults)

def main(argv):
    single_core = None
    multi_query = None
    start_date = None
    end_date = None
    csv_file = 0
    try:
        opts, args = getopt.getopt(argv,"hfm:s:e:c:t",["core=", "start=","end=", "multi="])
    except getopt.GetoptError:
        print 'weasl.py -s <start_date> -e <end_date>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'weasl.py -s <start_date> -e <end_date>'
            sys.exit()
        elif opt in ("-s", "--start"):
            start_date_arg = arg.split('-')
            start_date = datetime.date(int(start_date_arg[0]),int(start_date_arg[1]),int(start_date_arg[2]))
        elif opt in ("-e", "--end"):
            end_date_arg = arg.split('-')
            end_date = datetime.date(int(end_date_arg[0]),int(end_date_arg[1]),int(end_date_arg[2]))
        elif opt in ("-c", "--core"):
            single_core = arg
        elif opt in ("-f"):
            csv_file = 1
        elif opt in ("-m", "--multi"):
            multi_query = arg
        elif opt in ("-t"):
            get_cores_list() 

    if None not in (single_core, multi_query):
        dates_range = date_range(start_date, end_date)
        get_docs_single_core(single_core, dates_range)
    elif(csv_file):
        dates_range = date_range(start_date, end_date)
        get_docs_all_csv(dates_range)
    elif(multi_query):
        query_multi_core(multi_query)
    elif None not in (start_date, end_date):
        dates_range = date_range(start_date, end_date)
        get_docs_all_cores(dates_range)

if __name__ == "__main__":
       main(sys.argv[1:])
