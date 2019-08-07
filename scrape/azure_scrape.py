import json
import requests
import re
from datetime import datetime
from bs4 import BeautifulSoup
from dateutil.parser import parse
import sys
import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s', datefmt='%y%m%d %H:%M:%S')
logger = logging.getLogger()

MAIN_PAGE_URL = 'https://status.azure.com/en-us/status/history'
LINK_HEAD = 'https://status.azure.com/en-us/statushistoryapi/?'
STARTDATE = '20190312'  # YYYYMMDD  Just leave this as it is, the request url needs a start date.
PAGES = ['1']

TIME_REGEX = re.compile(r'((?:[01]?\d|2[0-3]):[0-5]\d)')
DATE_REGEX = re.compile(r'([0-3]?\d ....? 20..)')

RESULT_PATH = './data/azure_outage.json'

REQUEST_HEADERS = {  # to pretend to be a browser
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'Content-Type': 'application/x-www-form-urlencoded',
    'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36'
                   ' (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'),
    'Accept': ('text/html,application/xhtml+xml,'
               'application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8'),
    'Accept-Language': 'en-US,en;q=0.8,he;q=0.6',
}
OUTAGE_KEYWORD = ['outage', 'not be able', 'unable', 'fail', 'error', '500', '503', '403', '404']
DEGRADATION_KEYWORD = ['degradation', 'delay', 'latency', 'slow']
COLUMNS = ['issue', 'provider_type', 'provider', 'service', 'location', 'duration', 'affect_rate', 'impact', 'cause', 'intensity', 'time']
PROVIDER_TYPE = 'INFRASTRUCTURE_SERVICE_HOSTING'
PROVIDER = 'Azure'


class AzureScrape:
    def __init__(self):
        self.services = {}
        self.zones = {}
        self.events = {}  # dictionary to store event catalog

    def get_services_zones(self, main_page_url):
        """
        :param main_page_url: The url that leads us to the main azure incident page, where we can find all the zone and service options.
        :return: When creating filter in the request, azure uses service code and zone code like 'api-management' or 'w-india', we create two
        dictionaries that the key is the service/zone code and value is the text we need, this function changes the two dictionaries.
        """
        html = requests.get(main_page_url, headers=REQUEST_HEADERS)
        soup = BeautifulSoup(html.content, 'html.parser')
        options = soup.find(id='wa-dropdown-service').find_all('option')
        for option in options:
            # Create a map of service code to service text
            self.services[option['value']] = option.text

        options = soup.find(id='wa-dropdown-history-region').find_all('option')
        for option in options:
            # Create a map of zone code to zone text.
            self.zones[option['value']] = option.text

    @staticmethod
    def get_incidents(link):
        """
        :param link: Link to a page that has all the incidents of a given filters (selected zones and services)
        :return: A list of html tag object of incident
        """
        html = requests.get(link, headers=REQUEST_HEADERS)
        soup = BeautifulSoup(html.content, 'html.parser')
        return soup.find_all('div', attrs={'class': 'column small-11'})

    @staticmethod
    def get_impact(text):
        """
        :param text: The target text that we will try to look up keywords in.
        :return: If we find a keyword indicating outage, just return 'outage'. If nothing in outage is found, move on to degradation, finally if not a
        keyword return Nonetype object.
        """
        for outage_keyword in OUTAGE_KEYWORD:
            if outage_keyword in text:
                return 'outage'

        for degradation_keyword in DEGRADATION_KEYWORD:
            if degradation_keyword in text:
                return 'degradation'

        logger.warning(f'No impact keyword found')
        return None

    def scrape_existed_incident(self, issue, service, zone):
        """
        :param issue: The uniqee name of the issue
        :param service: The current service that the incident belongs to, in code form, not final text we need
        :param zone: The current Zone that the incident belongs to, in code form, not final text we need
        :return: Append zone and service to the same event in the event catalog
        """
        # if event duplicates, append the zone and service, only append service but not zone if the event region is global
        if 'Global' in self.events[issue]["location"]:
            if self.services[service] not in self.events[issue]["service"]:
                self.events[issue]["service"].append(self.services[service])

        else:
            if self.services[service] not in self.events[issue]["service"]:
                self.events[issue]["service"].append(self.services[service])
            if self.zones[zone] not in self.events[issue]["location"]:
                self.events[issue]["location"].append(self.zones[zone])

    def scrape_new_incident(self, incident, service, zone):
        """
        :param incident: The  paragraph of the current incident
        :param service: The current service that the incident belongs to, in code form, not final text we need
        :param zone: The current Zone that the incident belongs to, in code form, not final text we need
        :return: Create a new row in the event catalog with duration and impact
        """

        issue = incident.find('h3').text  # header for each events, we use it as a unique name to represent the incident

        target_paragraph = incident.find_all(lambda tag: tag.name == 'p' and 'Summary' in tag.text[0:21])
        summary = target_paragraph[-1].text if target_paragraph else incident.text
        time = TIME_REGEX.findall(summary.split(',')[0])  # search time in first sentence
        time = [datetime.strptime(t, '%H:%M') for t in time]
        date = DATE_REGEX.findall(summary.split(',')[0])  # search date in first sentence

        if len(date) == 0:  # Didn't find date in first sentence, go find in the whole summary text
            date = DATE_REGEX.findall(summary)
            duration = int((time[1] - time[0]).total_seconds() / 60)
            date = parse(date[0])
            starttime = datetime.strftime(datetime(date.year, date.month, date.day, time[0].hour, time[0].minute), '%Y-%m-%d %H:%M')

        elif len(date) == 1:  # Got one date, indicating the start time and end time are on the same date.
            duration = int((time[1] - time[0]).total_seconds() / 60)
            date = parse(date[0])
            starttime = datetime.strftime(datetime(date.year, date.month, date.day, time[0].hour, time[0].minute), '%Y-%m-%d %H:%M')

        else:  # Got more than one date, use the first 2
            datetimes = [parse(d) for d in date]  # date and times, but until this row only have date
            for i, date in enumerate(datetimes):
                datetimes[i] = datetime(date.year, date.month, date.day, time[i].hour, time[i].minute, 0)
            duration = int((datetimes[1] - datetimes[0]).total_seconds() / 60)
            starttime = datetime.strftime(datetimes[0], '%Y-%m-%d %H:%M')

        impact = self.get_impact(summary)
        cause = None
        event = {"provider_type": PROVIDER_TYPE, "provider": PROVIDER, "service": [self.services[service]], "duration": duration, "affect_rate": 0.5,
                 "impact": impact, "cause": cause, "intensity": None, "time": starttime}

        event["location"] = ['Global'] if zone == 'global' else [self.zones[zone]]

        self.events[issue] = event
        logger.info(f'Create new row {issue}')

    def scrape_incidents(self, incidents, service, zone):
        """
        :param incidents: The list of html tag object that contains information for all data
        :param service: The current service that the incident belongs to, in code form, not final text we need
        :param zone: The current Zone that the incident belongs to, in code form, not final text we need
        :return: It handles existed incident or new incident by calling different function that will further change the self event catalog.
        """
        for incident in incidents:
            issue = incident.find('h3').text  # header for each events, we use it as a unique name to represent the incident
            if issue in self.events:
                self.scrape_existed_incident(issue, service, zone)
            else:
                self.scrape_new_incident(incident, service, zone)

    def write_json(self):
        """
        :return: Add the key of the dictionary to the value of the dictionary and name it 'issue'. Write it to json file
        """
        logger.info('Done scraping.')
        logger.info('Storing data in json...')
        result = []
        for issue, dic in self.events.items():
            dic["issue"] = issue
            result.append(dic)

        with open(RESULT_PATH, 'w') as f:
            json.dump(result, f)

        logger.info(f'Done storing. File at:{RESULT_PATH}')

    def scrape(self):
        self.get_services_zones(main_page_url=MAIN_PAGE_URL)
        for service in self.services:
            # continue if service is 'all'. This page contains all incidents but makes no contribution to our parameter as we want to specify service.
            if service == 'all':
                continue
            logger.info(f'Now scraping {PROVIDER} {service}')

            # If we access a certain service's incident page that include "all" region, and it still returns an empty page, that means this service
            # has no historical incident
            url = f"{LINK_HEAD}serviceSlug={service}&regionSlug=all&startdate={STARTDATE}page=1"

            # We only need to check if there is incident under the 'all' region or not. If so, we record the number of incidents for further
            # comparison with the 'global' region one. If not we just skip.
            incidents = self.get_incidents(link=url)
            if not incidents:  # empty page, check next service
                continue
            else:
                len_all = len(incidents)

            # then we scrape all the global incident. if region:global returns only the same number as region:all, that means all the incidents on the
            # current service are global, then we don't need to spend time on checking specific region because they will all have the same incidents,
            url = f"{LINK_HEAD}serviceSlug={service}&regionSlug=global&startdate={STARTDATE}page=1"
            incidents = self.get_incidents(link=url)
            self.scrape_incidents(incidents, service, zone='global')
            if len(incidents) == len_all:
                continue

            for zone in self.zones:
                # We just accessed zone:all and zone:global 10 lines ago, so skip them
                if zone == 'all' or zone == 'global':
                    continue
                for PAGE in PAGES:
                    logger.info(f'Now scraping {PROVIDER} {service} {zone}')
                    url = f"{LINK_HEAD}serviceSlug={service}&regionSlug={zone}&startdate={STARTDATE}page={PAGE}"
                    incidents = self.get_incidents(link=url)
                    if not incidents:  # empty page, next zone/page
                        continue
                    self.scrape_incidents(incidents, service, zone)

        self.write_json()
