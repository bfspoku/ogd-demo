"""opendata.swiss (OGD) API demo harvester

This script illustrates a possible way to retrieve data resources referenced
on opendata.swiss OGD portal via the API. It makes use of HEAD requests to 
check if the resource has been updated before downloading it.
"""

import datetime
import json
import logging
import logging.handlers
import math
import re
import socket
import sys
import time
import urllib3
import requests
import pandas

# Opendata.swiss configuration
# baseurl is the URL of the API end point
# Here three examples: test, production and the local demo with local API and http server
#BASEURL = "https://ckan.ogdch-abnahme.clients.liip.ch/api/3/action/package_show?id=eidg-wahlen-2023" # Test (vor Wahlsonntag 00:00)
BASEURL = "https://ckan.opendata.swiss/api/3/action/package_show?id=eidg-wahlen-2023"                # Prod (am Wahlsonntag ab 12:00)
#BASEURL = "http://localhost:8000/eidg-wahlen-2023"                                                   # Test (local)

# The time between requests, for demo purpose quite short, should be higher in production (see Lieferintervalle)
POLLING_INTERVAL = 10

# Set the proxy settings based on naive IP detection
ipaddress = socket.gethostbyname(socket.gethostname())
if re.match('10.147', ipaddress)  or re.match('10.227', ipaddress) or re.match('10.194', ipaddress):        
    PROXIES = {
        "http"  :'http://proxy-bvcol.admin.ch:8080',
        "https" :'http://proxy-bvcol.admin.ch:8080'
    }
    HEADERS={
        'Referer': BASEURL,
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'
    }
    PROXY_GRACE_INTERVAL = 7
else:
    # Default settings for local demo
    PROXIES = None
    HEADERS = None
    PROXY_GRACE_INTERVAL = 0

logging.root.handlers = []
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
LOG_OFFSET = (len(LOG_FORMAT)-8)*" "
logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
    handlers=[
#        logging.FileHandler("debug.log", mode='a', delay=False),
        logging.handlers.TimedRotatingFileHandler(filename="harvester.log", when='D', interval=1, backupCount=30, encoding='utf-8', delay=False),
        logging.StreamHandler()
    ]
)

urllib3.disable_warnings() # Disable annoying HTTPS Warnings when using the proxy

# Dummy default timestamp for testing purpose (in case a resource has no timestamp)
DEFAULT_DATESTRING = 'Mon, 13 Oct 1975 09:30:00 GMT'

# Just a mapping table to display cantons acronyms instead of their FSO numbers
cantons = pandas.read_csv('cantons.csv', header=None, index_col=0)
cantons = cantons.squeeze("columns")
cantons = cantons.to_dict()


def get_urls(baseurl, session, proxies = None):
    """Retrieve the download URLs of the resources on opendata.swiss
    
    baseurl: the base url of the package containing the resources
    """
    urlist = []
    if re.search('localhost', baseurl) or re.search('127.0.0.1', baseurl):
        # deactivate proxy for API mockup
        proxies = None

    resources = session.get(
        baseurl,
        proxies = proxies,
        verify = False).json()['result']['resources']
    for resource in resources:
        urlist.append(resource['url'])
    logging.debug("%s Retrieved %s URLS from %s end point.", time.asctime(), len(urlist), baseurl)
    return urlist


def get_last_modified(url, proxies = None):
    """Return the last modified time of the resource as a timestamp"""
    r = requests.head(url, proxies=proxies, verify=False, headers=HEADERS, timeout=60)
    datestring = r.headers.get('Last-Modified', DEFAULT_DATESTRING)
    dt = datetime.datetime.strptime(datestring, "%a, %d %b %Y %H:%M:%S %Z")
    return int(time.mktime(dt.timetuple()))


def is_new(url, timestamps, proxies = None):
    """Return wether a ressource new is and updates the timestamps tracking accordingly"""
    ts = get_last_modified(url, proxies)
    if ts > timestamps.get(url, 0):
        timestamps[url] = ts
        return True
    else:
        return False


def print_status(url, session, timestamps):
    """Download the (json) and show some basic info about the progress, just for the demo"""
    filename = url.split("/")[-1]

    t0 = time.time()
    response = session.get(
        url,
        proxies = PROXIES,
        headers = HEADERS,
        verify = False)
    t1 = time.time()
    download_time = int(t1 - t0)

    filesize = int(int(response.headers['Content-Length'])/1024)

    try:
        t0 = time.time()
        jsondata = json.loads(response.content)
        t1 = time.time()
        load_time = int(t1 - t0)
    except:
        logging.error("JSON invalid?")
        return

    if 'timestamp' in jsondata:
        time_created = jsondata['timestamp']
        timestamp_uploaded = timestamps.get(url, '0')
        dt = datetime.datetime.strptime(time_created, "%Y-%m-%dT%H:%M:%S")
        time_created = time.strftime("%d.%m.%Y %H:%M:%S", dt.timetuple())
        dt = dt + datetime.timedelta(hours=-2) # timestamp ist in CET in file, in UTC in response
        timestamp_created = int(time.mktime(dt.timetuple()))
        time_in_system = timestamp_uploaded - timestamp_created
#        logging.info("%s created %s, size = %skb, time to availability = %ss, download time = %ss, load time = %ss", filename,  time_created, filesize, timestamp_uploaded-timestamp_created, download_time, load_time)
        logging.info("%s creation time  %s CET", filename, time_created)
        logging.info("%s size           %s kb", filename, filesize)
        logging.info("%s time in system %s'%s\"", filename, time_in_system//60, time_in_system%60)
        logging.info("%s download time  %s\"", filename, download_time if download_time else "<1")
        #logging.info("%s load time     %s\"", filename, load_time)
    else:
        logging.warning("%s has no timestamp.", filename)

    if 'stand_kantone' in jsondata:
        cantons_results = jsondata['stand_kantone']
        for canton_result in cantons_results:
            canton = cantons.get(canton_result.get('kanton_nummer', None), '??')
            progress = math.floor(100 * canton_result.get('gemeinden_abgeschlossen', -1) / canton_result.get('gemeinden_total', -1))
            if progress:
                logging.info("%s %s %3.0f%%", filename, canton, progress)


def update(urls, session, timestamps, proxies = None):
    """Check if a resource has been updated and downloads it if its newer
    
    Once downloaded, the resource can be analysed directly or saved somewhere. Here we only print some
    basic stats about it for demo purposes
    """
    for url in urls:
        timestamp = datetime.datetime.fromtimestamp(timestamps.get(url, int(time.time()))).strftime('%c')
        if url and is_new(url, timestamps, proxies):
            logging.info("%s changed", url)
            print_status(url, session, timestamps)
        else:
            logging.debug("%s unchanged", url)
        if proxies:
            # some proxies block HEAD request if they come too close to each other
            time.sleep(PROXY_GRACE_INTERVAL) 


if __name__ == "__main__":
    # A single session to use for the requests
    # NOTE: not systematically implemented because of the mess with the proxy settings
    # TODO: actually use the single session everywhere if possible.
    sess = requests.Session()

    # Dictionary to keep track of the timestamps
    timestamp_list = dict()

    print("Press Ctrl+C to end the loop.")
    while True:
        # 1. Retrieve the URLS
        # NOTE: this doesn't necessary need to happen each iteration,
        #       one could retrieve the URLs only if the download fails for example
        try:
            url_list = get_urls(BASEURL, sess, proxies = PROXIES)
        except requests.exceptions.ConnectionError:
            logging.error("Error connecting to %s. If you are running the API mockup locally, check that httpserver.py is running.", BASEURL)
            sys.exit()

        logging.debug("%s Sending %s HEAD request(s).", time.asctime(), len(url_list))
        try:
            update(url_list, sess, timestamp_list, proxies = PROXIES)
        except KeyboardInterrupt:
            break
        time.sleep(POLLING_INTERVAL)
