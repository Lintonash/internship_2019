import requests
import lxml.html
import os
import re
from datetime import datetime
import json
import sys
import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s', datefmt='%y%m%d %H:%M:%S')
logger = logging.getLogger()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULT_PATH = 'data/gcp_outage.json'

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

DOMAIN = 'https://status.cloud.google.com/incident/'

SERVICES = ['appengine', 'compute', 'storage', 'bigquery', 'cloud-ddatastore', 'cloud-dev-tools', 'cloud-functions',
            'cloud-iam', 'cloud-ml', 'cloud-networking', 'cloud-pubsub', 'cloud-sql', 'cloud-dataflow',
            'container-engine', 'google-stackdriver', 'developers-console', 'support']

ZONES = ['asia-east1', 'asia-east2', 'asia-northeast1', 'asia-south1', 'asia-southeast1', 'australia-southeast1',
         'europe-north1', 'europe-west1', 'europe-west2', 'europe-west3', 'europe-west4', 'europe-west6',
         'northamerica-northeast1', 'southamerica-east1', 'us-central1', 'us-east1', 'us-east4', 'us-west1', 'us-west2']

COUNTRIES = ['Asia', 'North America', 'South America', 'Europe', 'US multi-region', 'US multi region', 'European multi-region',
             'Canada']

YEARS = ['15', '16', '17', '18', '19']

ISSUES = ['001', '002', '003', '004', '005', '006', '007', '008', '009', '010', '011', '012', '013', '014',
          '015', '016', '017', '018', '019', '020', '021', '022', '023', '024', '025', '026', '027', '028', '029',
          '030', '031', '032', '033', '034', '035', '036', '037', '038', '039', '040', '041', '042', '043', '044',
          '045', '046', '047', '048', '049', '050', '051', '052', '053', '054', '055', '056', '057', '058', '059',
          '060', '061', '062', '063', '064', '065', '066', '067', '068', '069', '070', '071', '072', '073', '074',
          '075', '076', '077', '078', '079', '080', '081', '082', '083', '084', '085', '086', '087', '088', '089',
          '090', '091', '092', '093', '094', '095', '096', '097', '098', '099']

OUTAGE_KEYWORD = ['outage', 'not be able', 'unable', 'error', 'fail', '500', '503', '403', '404']
DEGRADATION_KEYWORD = ['degradation', 'delay', 'latency', 'slow']
PROVIDER_TYPE = 'INFRASTRUCTURE_SERVICE_HOSTING'
PROVIDER = 'GCP'

events = []  # list to store event catalog


def get_impact(doc):
    """
    :param doc: The lxml
    :return: 'outage', 'degradation', or Nonetype object
    """
    # bubble is a round icon on the incident page that indicates how severe the incident was.
    bubbles = set()
    table = doc.xpath('//*[@id="maia-main"]/table')
    for i in table[0]:
        for j in i.getiterator():
            if j.values() and 'bubble' in j.values()[0] and j.values()[0] != 'bubble ok':
                bubbles.add(j.values()[0])
    bubbles = list(bubbles)
    result = None
    if "bubble high" in bubbles or "bubble outage" in bubbles:
        result = 'outage'

    elif "bubble medium" in bubbles:
        result = "degradation"

    else:
        logger.warning(f'Unclear impact with {bubbles}')

    return result


def get_duration_and_starttime(doc):
    """
    :param doc: The lxml
    :return: duration in minutes and a start time in format '%Y-%m-%d %H:%M'
    example: output: 270, '2017-09-08 12:34'
    """

    time = doc.xpath('//*[@id="maia-main"]/div[2]/p/strong')
    if not time:
        time = doc.xpath('//*[@id="maia-main"]/div[3]/p/strong')

    start = time[0].text
    end = time[1].text

    # To handle no dash typo.
    dash = '-' if '-' in start else '/'
    start_time = datetime.strptime(start, f'%Y{dash}%m{dash}%d %H:%M')
    end_time = datetime.strptime(end, f'%Y{dash}%m{dash}%d %H:%M')

    result = int((end_time - start_time).total_seconds() / 60)

    return result, datetime.strftime(start_time, '%Y-%m-%d %H:%M')


def get_location_and_percentage(doc):
    """
    :param doc: The lxml
    :return: A list of location and a list of string of percengate number
    example: output: ['us-east', 'us-west-2'], ['2.5%', '10%', '15%']
    """
    second_title = doc.xpath('//*[@id="maia-main"]/div[1]/p')[0].text

    description = ''
    table = doc.xpath('//*[@id="maia-main"]/table')
    for i in table[0]:
        for j in i.getiterator():
            if j.values() and j.values()[0] not in description:
                description += (j.values()[0])

    # The priority of searching zones is to search zones name in second_title, if nothing we go search zones name in description, if still nothing,
    # we search again with countries name
    result_location = re.findall(f"{'|'.join(ZONES)}", second_title)
    if not result_location:
        result_location = re.findall(f"{'|'.join(ZONES)}", description)
    if not result_location:
        result_location = re.findall(f"{'|'.join(COUNTRIES)}", second_title)
    if not result_location:
        result_location = re.findall(f"{'|'.join(COUNTRIES)}", description)

    result_percentage = re.findall("\\d+.\\d+%|\\d+%", second_title)
    if not result_percentage:
        result_percentage = re.findall("\\d+.\\d+%|\\d+%", description)

    return result_location, result_percentage


def calculate_affect_rate(numbers):
    """
    :param numbers: The list of string of percentage numbers
    :return: The average (float) of those numbers

    example:
    input: ['2.5%', '5-10%', '1~2%']
    output: 0.038 (the average of 2.5%, 7.5%, and 1.5%)
    """
    if not numbers:
        return None

    total = 0
    for number in numbers:
        # Handle the '-' and '~', see example in function documentation
        if '-' in number:
            for num in number.split('-'):
                total += float(num.replace('%', '')) / 100
        elif '~' in number:
            for num in number.split('~'):
                total += float(num.replace('%', '')) / 100
        else:
            total += float(number.replace('%', '')) / 100
    avg = total/len(numbers)
    return round(avg, 3)


def write_json():
    """
    :return: Take the global events and write to json file
    """
    with open(RESULT_PATH, 'w') as f:
        json.dump(events, f)
    logger.info(f'json file at: {RESULT_PATH}')


def scrape():
    """
    :return: Iterate through all the SERVICES, YEARS and ISSUES, get all the information and store to json file
    """
    for SERVICE in SERVICES:
        for YEAR in YEARS:
            for ISSUE in ISSUES:
                logger.info(f'Now scraping {PROVIDER} {SERVICE + YEAR + ISSUE}')
                parsed_link = f'{DOMAIN}{SERVICE}/{YEAR}{ISSUE}'

                html = requests.get(parsed_link, headers=REQUEST_HEADERS)

                if not html.ok:
                    break

                lxml_doc = lxml.html.fromstring(html.content)
                impact = get_impact(doc=lxml_doc)
                if not impact:
                    continue

                # Google sometime forget to follow up an event
                try:
                    duration, starttime = get_duration_and_starttime(lxml_doc)
                except Exception:
                    logger.exception(f"Fail to get duration of {parsed_link}")
                    continue

                location, percentage = get_location_and_percentage(lxml_doc)

                affect_rate = calculate_affect_rate(percentage)

                row = {"issue": f'{SERVICE}{YEAR}{ISSUE}',
                       "provider_type": PROVIDER_TYPE,
                       "provider": PROVIDER,
                       "service": SERVICE,
                       "location": location,
                       "duration": duration,
                       "affect_rate": affect_rate,
                       "impact": impact,
                       "cause": None, "intensity": None,
                       "time": starttime}

                events.append(row)

    logger.info('Done scraping.')
    write_json()
