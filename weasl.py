#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys, getopt
import os
import datetime
import urllib2
import csv
from datetime import timedelta
from elementtree import ElementTree
from bs4 import BeautifulSoup
from itertools import tee, islice, chain, izip

def previous_and_next(some_iterable):
    prevs, items, nexts = tee(some_iterable, 3)
    prevs = chain([None], prevs)
    nexts = chain(islice(nexts, 1, None), [None])
    return izip(prevs, items, nexts)

# Returns the entire list of cores.
# TODO: Use /solr/admin/cores?action=STATUS to get an xml result.  
# That would allow me to get rid of Beautiful Soup and just use ElementTree
def get_cores_list(admin_url):
    solr_admin_page = urllib2.urlopen(admin_url)
    admin_html = solr_admin_page.read()
    admin_soup = BeautifulSoup(admin_html)
    cores_list = []
    for link in admin_soup.find_all('a'):
        if(link.get('href').split('/')[0] != "DEFAULT" and link.get('href').split('/')[0] != "." and link.get('href').split('/')[0] != "gitmo" ):
            cores_list.append(link.get('href').split('/')[0])
    return cores_list

def date_range(start, end):
    r = (end+datetime.timedelta(days=1)-start).days
    return [start+datetime.timedelta(days=i) for i in range(r)]

def get_docs_all_cores(timestamps):
    cores = get_cores_list('http://rslr006p.nandomedia.com:8983/solr/')
    for core in cores:
        print "\n" + core 
        for previous, item, next in previous_and_next(timestamps):
            if(next):
                url_string = 'http://rslr006p.nandomedia.com:8983/solr/{0}/select/?q=pubsys_asset_creation_dt%3A%5B{1}+TO+{2}%5D&start=0&rows=1'.format(core, item.strftime("%s"), next.strftime("%s"))
                tree = ElementTree.parse(urllib2.urlopen(url_string))
                rootElem = tree.getroot().find('result')
                print item.strftime("%Y-%m-%d") + ": " + rootElem.attrib.get('numFound')

def get_docs_all_csv(timestamps):
    cores = get_cores_list('http://rslr006p.nandomedia.com:8983/solr/')
    rows = []
    header_row = list(cores)
    header_row.insert(0,"Date")
    rows.append(header_row)
    for previous, item, next in previous_and_next(timestamps):
        data_row = []
        if(next):
            data_row.append(item.strftime("%Y-%m-%d"))
            for core in cores:
                url_string = 'http://rslr006p.nandomedia.com:8983/solr/{0}/select/?q=pubsys_asset_creation_dt%3A%5B{1}+TO+{2}%5D&start=0&rows=1'.format(core, item.strftime("%s"), next.strftime("%s"))
                tree = ElementTree.parse(urllib2.urlopen(url_string))
                rootElem = tree.getroot().find('result')
                data_row.append(rootElem.attrib.get('numFound'))
        rows.append(data_row)
    with open('solr_docs.csv', 'wb') as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    print "Done!"

def get_docs_single_core(core, timestamps):
    for previous, item, next in previous_and_next(timestamps):
        if(next):
            url_string = 'http://rslr006p.nandomedia.com:8983/solr/{0}/select/?q=pubsys_asset_creation_dt%3A%5B{1}+TO+{2}%5D&start=0&rows=1'.format(core, item.strftime("%s"), next.strftime("%s"))
            tree = ElementTree.parse(urllib2.urlopen(url_string))
            rootElem = tree.getroot().find('result')
            print item.strftime("%Y-%m-%d") + ": " + rootElem.attrib.get('numFound')

def query_multi_core(query):
    cores = get_cores_list('http://rslr006p.nandomedia.com:8983/solr/')
    results = []
    numResults = 0
    for core in cores:
        url_string = 'http://rslr006p.nandomedia.com:8983/solr/{0}/select/?q={1}'.format(core, query)
        tree = ElementTree.parse(urllib2.urlopen(url_string))
        rootElem = tree.getroot().find('result')
        print "\n" + core + ": " + url_string
        print  "Results: " + rootElem.attrib.get('numFound')
        numResults += int(rootElem.attrib.get('numFound'))
    print "Total Results across all cores: " + str(numResults)

def main(argv):
    single_core = None
    multi_query = None
    csv_file = 0
    try:
        opts, args = getopt.getopt(argv,"hfm:s:e:c:",["core=", "start=","end=", "multi="])
    except getopt.GetoptError:
        print 'weasl.py -s <start_date> -e <end_date>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'weasl.py -s <YYYY-MM-DD> -e <YYYY-MM-DD>'
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

    if None not in (single_core, multi_query):
        dates_range = date_range(start_date, end_date)
        get_docs_single_core(single_core, dates_range)
    elif(csv_file):
        dates_range = date_range(start_date, end_date)
        get_docs_all_csv(dates_range)
    elif(multi_query):
        query_multi_core(multi_query)
    else:
        dates_range = date_range(start_date, end_date)
        get_docs_all_cores(dates_range)

if __name__ == "__main__":
       main(sys.argv[1:])
