import json
from abc import ABCMeta, abstractmethod
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re
import sys
import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s', datefmt='%y%m%d %H:%M:%S')
logger = logging.getLogger()

UNWANT_IMPACT = ['maintenance']  # , 'none']
PAGE_RANGE = range(1, 41)
BLANK_COUNTER_LIMIT = 3
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


class StatusPageScraper(metaclass=ABCMeta):
    def __init__(self, name):
        self.provider = name
        self.result_path = f'data/{name.lower()}_outage.json'
        self.events = []

        # if get_incidents returns a few 'none' in a row, then we conclude that there is no more information from this page on.
        self.blank_counter = 0
        self.time_redundant = ['UTC', 'PST', 'PDT', 'th', 'TH', '(', ')']  # in the time text, there are some redundant word that needs to be delete

    @staticmethod
    def get_incidents_and_year(link):
        """
        :param link: a full url that lead to a history page of the provider using statuspage.
        :return: this function accesses the link and retrieve the incident dictionaries, put them in a list, return the list along with the
        year(string) if the page has zero incident, return None
        """
        incident_flag = False  # a flag to check if this page has even one incident, if so, it will be set to True
        result_incidents = []
        html = requests.get(link, headers=REQUEST_HEADERS)
        soup = BeautifulSoup(html.content, 'html.parser')
        div = soup.find('div', {'data-react-class': 'HistoryIndex'})

        data_react_props = div.get('data-react-props')
        months = json.loads(data_react_props)['months']

        for month in months:
            for incident in month['incidents']:
                incident_flag = True
                if incident['impact'] not in UNWANT_IMPACT:
                    result_incidents.append(incident)
        result_year = months[0]['year']
        return result_incidents, result_year, incident_flag

    def get_duration_and_starttime(self, incident, year):
        """
        :param incident: a dictionary that contains all the information about this incident, including timestamp.
        :param year: the incident year we retrieved earlier, because the timestamp doesn't include any year information
        :return: return the duration (integer) in minutes, and the start time in a fixed format (string)

        example:
        input: timestamp = 'Jun 27, 06:13 - 07:00 PDT', year = '2019'
        output: 47, '2019-06-27 06:13'
        """
        timestamp = incident['timestamp']
        timetext = BeautifulSoup(timestamp, 'html.parser').text
        for word in self.time_redundant:
            timetext = timetext.replace(word, '')

        # Past events will have a '-' connecting start time and end time, but on-going event doesn't, so check to handle this
        if '-' not in timetext:
            logger.warning(f'Investagating incident: {self.provider} {incident["code"]}')
            return 0, None

        start_time, end_time = timetext.strip().split('-')

        # Convert start_time from string to datetime object
        start_time = datetime.strptime(f'{year} {start_time.strip()}', '%Y %b %d, %H:%M')

        # if ',' in end_time, it means end_time is on a different date of start_time
        if ',' in end_time:
            end_time = datetime.strptime(f'{year} {end_time.strip()}', '%Y %b %d, %H:%M')

        else:
            # else end_time and start_time on the same date, then use start_time's Year Month Date to convert end_time to datetime object
            end_time = datetime.strptime(f'{datetime.strftime(start_time, "%Y %b %d")}, {end_time.strip()}', '%Y %b %d, %H:%M')

        result = (end_time - start_time).total_seconds() / 60

        return result, datetime.strftime(start_time, '%Y-%m-%d %H:%M')

    @staticmethod
    def get_impact(incident):
        if incident['impact'] == 'none':
            result = None
        elif incident['impact'] == 'minor':
            result = 'degradation'
        else:
            result = 'outage'

        return result

    def scrape(self):
        """
        :return: Iterate all pages and all incidents on every page, get all the variables we need and append a dictionary to self.events
        """

        for page in PAGE_RANGE:
            logger.info(f'Now scraping {self.provider} page {page}')

            if self.blank_counter == BLANK_COUNTER_LIMIT:
                logger.info(f'No more information since page {page - (BLANK_COUNTER_LIMIT + 1)}')
                break

            url = f"{self.history_page_head}{page}"
            incidents, year, incident_flag = self.get_incidents_and_year(url)

            # Update blank counter. If we get a non-empty page, we reset the counter. If we get an empty page, add 1.
            self.blank_counter = 0 if incident_flag else self.blank_counter + 1

            for incident in incidents:
                logger.info(f'Now scraping incident: {incident["code"]}')
                # get duration and start time
                duration, starttime = self.get_duration_and_starttime(incident=incident, year=year)

                impact = self.get_impact(incident=incident)

                location, service, provider_type = self.get_location_service_and_provider_type(incident['code'])

                event = {'issue': incident['code'], "provider_type": provider_type, "provider": self.provider, "service": None, "location": location,
                         "duration": duration, "affect_rate": None, "impact": impact, "cause": None, "intensity": None, "time": starttime}

                self.events.append(event)

    def write_json(self):
        """
        :return: Takes the list events and output it as a json file
        """
        with open(self.result_path, 'w') as f:
            json.dump(self.events, f)

    @property
    @abstractmethod
    def history_page_head(self):
        pass

    @property
    @abstractmethod
    def incident_page_head(self):
        pass

    @abstractmethod
    def get_location_service_and_provider_type(self, hash_code):
        pass


class CloudflareScraper(StatusPageScraper):
    history_page_head = 'https://www.cloudflarestatus.com/history?page='
    incident_page_head = 'https://www.cloudflarestatus.com/incidents/'
    location_regex = re.compile(r'[,(](.*?) - \([A-Z]+\)')

    def get_location_service_and_provider_type(self, hash_code):
        """
        :param hash_code: a string that represents a Cloudflare incident, we use it to generate the url of the incident page
        :return: This function access the incident page, retrieve the location affected and check if 'CDN' in the text (to decide provider_type).
        We return the locations as a list of string and a string of provider_type
        """
        link = f"{self.incident_page_head}{hash_code}"
        response = requests.get(link, headers=REQUEST_HEADERS)
        soup = BeautifulSoup(response.content, 'html.parser')

        affected_component = soup.find('div', {'class': "components-affected font-small color-secondary border-color"})
        if not affected_component:
            logger.warning(f'No location and service in incident: {self.provider} {hash_code}')
            return [], None, 'INFRASTRUCTURE_SERVICE_DNS'

        if 'CDN' in affected_component.text:
            result_provider_type = 'INFRASTRUCTURE_SERVICE_CONTENT_DELIVERY_NETWORK'
        else:
            result_provider_type = 'INFRASTRUCTURE_SERVICE_DNS'

        result_location = self.location_regex.findall(affected_component.text)
        result_location = [place.strip() for place in result_location]

        return result_location, None, result_provider_type


class SAPScraper(StatusPageScraper):
    history_page_head = 'https://sapcp.statuspage.io/history?page='
    incident_page_head = 'https://sapcp.statuspage.io/incidents/'
    redundant_service_regex = re.compile(r'\[.*?\]')  # in service, there are some redundant text that needs to be substitute

    def get_location_set_and_service_set(self, link):
        """
        :param link: Link to the SAP history page, where we can find the filter options and generate location set and service set
        :return: Two set
        """
        result_location_set = []
        result_service_set = []
        html = requests.get(link, headers=REQUEST_HEADERS)
        soup = BeautifulSoup(html.content, 'html.parser')
        div = soup.find('div', {'data-react-class': 'HistoryIndex'})
        data_react_props = div.get('data-react-props')
        components = json.loads(data_react_props)['components']

        # There is a value called 'group', if it is true, that means this component is a location, if it is false, that means this component is a
        # service name under that location, we use regex to get rid of the region.
        for component in components:
            if component['group'] and 'Identity Authentication' not in component['name']:
                result_location_set.append(component['name'].strip())
            else:
                result_service_set.append(component['name'].strip())

            # The following line handles redundant text in the service text, i.e "App Engine [India] - Asia" --> "App Engine"
            result_service_set = [self.redundant_service_regex.sub('', service).split('-')[0].strip() for service in result_service_set]

        return set(result_location_set), set(result_service_set)

    def get_location_service_and_provider_type(self, hash_code):
        """
        :param hash_code: The hash code leading to the incident detail page
        :return: A list of location and a list of service
        """
        location_set, service_set = self.get_location_set_and_service_set(f'{self.history_page_head}1')
        result_location = []
        result_service = []
        link = f"{self.incident_page_head}{hash_code}"
        response = requests.get(link, headers=REQUEST_HEADERS)
        soup = BeautifulSoup(response.content, 'html.parser')

        affected_component = soup.find('div', {'class': "components-affected font-small color-secondary border-color"})
        if not affected_component:
            logger.warning(f'No location and service in incident: {self.provider} {hash_code}')
            return result_location, result_service, "INFRASTRUCTURE_SERVICE_HOSTING"

        for location in location_set:
            if location in affected_component.text:
                result_location.append(location)

        for service in service_set:
            if service in affected_component.text:
                result_service.append(service)

        return result_location, result_service, "INFRASTRUCTURE_SERVICE_HOSTING"


class OracleScraper(StatusPageScraper):
    history_page_head = 'https://ocistatus.oraclecloud.com/history?page='
    incident_page_head = 'https://ocistatus.oraclecloud.com/incidents/'
    start_time_regex = re.compile(r'start time: (.*?)utc')
    end_time_regex = re.compile(r'end time: (.*?)utc')

    @staticmethod
    def get_impact(incident):
        """
        :param incident: the incident dictionary that contains basic information
        :return: The criteria to define impact of Oracle is different from the other two, therefore we rewrite the method
        """
        result = 'degradation' if incident['impact'] == 'none' else 'outage'
        return result

    def get_duration_and_starttime(self, incident, year):
        """
        :param incident: the incident dictionary that contains basic information
        :param year: string of a year that the incident belongs to
        :return: a duration in minutes and a starttime. In Oracle statuspage, if the duration caculated from timestamp is 0, we can go deeper to the
        incident page to retrieve real starttime and endtime, therefore, we rewrite this method for Oracle.
        """
        result_duration, result_starttime = super().get_duration_and_starttime(incident=incident, year=year)

        # If the duration returned by the father function is longer than 0, just use it and no need to go deeper.
        if result_duration > 0:
            return result_duration, result_starttime

        link = f"{self.incident_page_head}{incident['code']}"
        response = requests.get(link, headers=REQUEST_HEADERS)
        soup = BeautifulSoup(response.content, 'html.parser')
        textblock = soup.find(lambda tag: tag.name == 'div' and 'start time' in tag.text.lower(), attrs={'class': "update-body font-regular"})
        if not textblock:
            logger.warning(f'No duration in incident: {self.provider} {incident["code"]}')
            return -1, None

        # If we can find a textblock, proceed, but Oracle staff has no consistency regarding upper or lower case
        start_time = self.start_time_regex.findall(textblock.text.lower())
        end_time = self.end_time_regex.findall(textblock.text.lower())

        # if both regex findall functions get their own string, proceed to calculate duration
        if start_time and end_time:
            start_time = start_time[0].strip()
            end_time = end_time[0].strip()

        # if Oracle got the format wrong, (e.g. two START TIME), handle it
        elif not end_time and len(start_time) == 2:
            end_time = start_time[1].strip()
            start_time = start_time[0].strip()

        else:
            logger.warning(f'No duration in incident {self.provider} {incident["code"]}')
            return -1, None

        for word in self.time_redundant:
            start_time = start_time.replace(word, '').strip()
            end_time = end_time.replace(word, '').strip()

        # Oracle staff has no consistency about whether to use comma and double dot.
        doubledot = ':' if ':' in start_time else ''
        comma = ',' if ',' in start_time else ''
        start_time = datetime.strptime(start_time, f'%B %d{comma} %Y %H{doubledot}%M')
        end_time = datetime.strptime(end_time, f'%B %d{comma} %Y %H{doubledot}%M')

        result = (end_time - start_time).total_seconds() / 60

        return result, datetime.strftime(start_time, '%Y-%m-%d %H:%M')

    def get_location_service_and_provider_type(self, hash_code):
        """
        :param hash_code: a string that represents a Oracle incident, we use it to generate the url of the incident page
        :return: This function access the incident page, retrieve the service and location affected, get the provider type. We return three variates,
        result_service(string) and result_location(list of string), and result_provider_type(string)
        """
        link = f"{self.incident_page_head}{hash_code}"
        response = requests.get(link, headers=REQUEST_HEADERS)
        soup = BeautifulSoup(response.content, 'html.parser')

        affected_component = soup.find('div', {'class': "components-affected font-small color-secondary border-color"})

        if affected_component:
            result_service = re.findall(r':(.*)\(', affected_component.text)[0].strip()
            result_location = re.findall(r'\((.*)\)', affected_component.text)[0].split('(')[-1].replace('region', '').split(',')
            result_location = [place.strip() for place in result_location]

        else:
            textblock = soup.find(lambda tag: tag.name == 'div' and 'service' in tag.text.lower() and 'region' in tag.text.lower(),
                                  attrs={'class': "update-body font-regular"})
            if not textblock:
                logger.warning(f'No location and service in incident: {self.provider} {hash_code}')
                return [], None, 'INFRASTRUCTURE_SERVICE_HOSTING'

            result_service = re.findall(r'services?\(?s?\)?: (.*?)region', textblock.text.lower())
            if result_service:
                result_service = result_service[0].strip()
            else:
                result_service = None
                logger.warning(f"No service in incident {self.provider} {hash_code}")

            result_location = re.findall(r'[Rr][Ee][Gg][Ii][Oo][Nn]S?\(?S?\)?: (.*?)[B-Z]', textblock.text)  # in case "region: All", 'A' is not end.
            if result_location:
                result_location = result_location[0].split(',')
                result_location = [place.strip() for place in result_location]

            else:
                result_location = []
                logger.info(f"No location in incident {self.provider} {hash_code}")

        result_provider_type = 'INFRASTRUCTURE_SERVICE_DNS' if result_service and 'DNS' in result_service else 'INFRASTRUCTURE_SERVICE_HOSTING'
        return result_location, result_service, result_provider_type


if __name__ == '__main__':
    cloudflare_scraper = CloudflareScraper(name='Cloudflare')
    sap_scraper = SAPScraper(name='SAP')
    oracle_scraper = OracleScraper(name='Oracle')

    cloudflare_scraper.scrape()
    sap_scraper.scrape()
    oracle_scraper.scrape()
