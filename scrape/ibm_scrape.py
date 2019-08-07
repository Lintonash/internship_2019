import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import re
import sys
import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s', datefmt='%Y%m%d %H:%M:%S')
logger = logging.getLogger()


OUTAGE_KEYWORD = ['outage', 'not be able', 'unable', 'error', 'fail', '500', '503', '403', '404']
DEGRADATION_KEYWORD = ['degradation', 'delay', 'latency', 'slow']
DURATION_THRESHOLD = 15
RESULT_PATH = 'data/ibm_outage.json'
PROVIDER_TYPE = 'INFRASTRUCTURE_SERVICE_HOSTING'
PROVIDER = 'IBM'
URL = 'https://cloud.ibm.com/status/api/notifications/feed.rss'

ISSUE_REGEX = re.compile(r'ID:(.*?) {2}')
SERVICE_REGEX = re.compile(r'Resources:(.*?) {2}')
LOCATION_REGEX = re.compile(r'Regions:(.*?) {2}')
START_TIME_REGEX = re.compile(r'Outage Start:(.*?) {2}')
END_TIME_REGEX = re.compile(r'Outage End:(.*?) {2}')
events = []


def get_incidents(link):
    """
    :param link: The link of the IBM incident page
    :return: A list of html element object, each of them is called 'item', we will use soup.find_all on this 'item' later.
    """
    html = requests.get(link)
    soup = BeautifulSoup(html.content, 'lxml')
    result = soup.find_all('item')
    return result


def get_issue(text):
    """
    :param text: The target text that we will apply regex findall upon
    :return: If we find somenthing, then return the ID in string form, otherwise will return a Nonetype object
    """
    result = ISSUE_REGEX.findall(text)
    if result:
        logger.info(f'Now scraping incident: {result[0].strip()}')
        return result[0].strip()

    logger.warning("Couldn't get issue")
    return result


def get_service(text):
    """
    :param text: The target text that we will apply regex findall upon
    :return: If we find somenthing, then return the service in string form, otherwise will return a Nonetype object
    """
    result = SERVICE_REGEX.findall(text)
    if result:
        return result[0].strip()

    logger.warning("Couldn't get service")
    return result


def get_location(text):
    """
    :param text: The target text that we will apply regex findall upon
    :return: If we find somenthing, then return the location in string form, otherwise will return a Nonetype object
    """
    result = LOCATION_REGEX.findall(text)
    if result:
        return result[0].strip()

    logger.warning("Couldn't get location")
    return result


def get_duration_and_start_time(text):
    """
    :param text: The target text that we will apply regex findall upon
    :return: If we find somenthing, then return the duration in minutes and start time
    """
    start_time = START_TIME_REGEX.findall(text)
    end_time = END_TIME_REGEX.findall(text)
    if not start_time or not end_time:
        logger.warning("Couldn't get duration")
        return -1, None

    start_time = datetime.strptime(start_time[0].strip(), '%a %b %d %Y %H:%M:%S %Z%z')
    end_time = datetime.strptime(end_time[0].strip(), '%a %b %d %Y %H:%M:%S %Z%z')
    result = int((end_time-start_time).total_seconds() / 60)
    return result, datetime.strftime(start_time, '%Y-%m-%d %H:%M')


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


def write_json():
    """
    :return: Take the global events and write to a json file
    """
    with open(RESULT_PATH, 'w') as f:
        json.dump(events, f)


if __name__ == '__main__':
    incidents = get_incidents(URL)
    logger.info(f'Now scraping {PROVIDER}')
    for incident in incidents:
        description = incident.find('description').text
        incident_type = re.findall(r'Type:(.*?) {2}', description)[0].strip()

        # Exclude 'maintenance'
        if not incident_type == 'incident':
            continue

        issue = get_issue(description)  # unique name to identify this issue

        duration, starttime = get_duration_and_start_time(description)
        if duration <= DURATION_THRESHOLD:
            continue

        service = get_service(description)

        location = get_location(description)

        impact = get_impact(description)

        # These three are important, if anyone of them is None, continue, but we can accept None in impact
        if None in [issue, service, location]:
            continue

        event = {"issue": issue, "provider_type": PROVIDER_TYPE, "provider": PROVIDER, "service": service, "location": location, "duration": duration,
                 "affect_rate": None, "impact": impact, "cause": None, "intensity": None, "time": starttime}
        events.append(event)

    write_json()
    logger.info(f'Done scraping {PROVIDER}')
