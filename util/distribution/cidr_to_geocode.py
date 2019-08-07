from netaddr import IPNetwork
import netaddr
import requests
import random
import csv
import json
from collections import Counter, defaultdict
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s', datefmt='%y%m%d %H:%M:%S')
logger = logging.getLogger()

with open('input/us_state_abbreviations.json', 'r') as f:
    US_STATE_ABBREVIATIONS = json.load(f)

with open('input/server_to_region.json', 'r') as f:
    SERVER_TO_REGION = json.load(f)

with open('input/geocode.json') as f:
    GEOCODE = json.load(f)

IPWHOIS_URL = 'https://pro.ipwhois.io/json'
IPWHOIS_KEY = 'WD2SYIax4wvAVvY3'
SAMPLE_NUMBER = 20
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

cidr_to_geocode_map = defaultdict(dict)
counters = defaultdict(lambda: defaultdict(int))  # to record distribution
detect = {}  # to record non-ip cidr-to-region map


# When trying to get state code from a private ip that we know it is in the US, we will assign a random state code to it based on option
def generate_location_option():
    """
    :return: This function reads from the provider_catalog.csv file and use the location columns in it to generate location option for future use. It
    returns a dictionary
    """
    result = defaultdict(dict)
    provider_catalog_path = '../../distribution/input/provider_catalog.csv'

    with open(provider_catalog_path, 'r', encoding='utf-8-sig') as f:
        provider_catalog = [row for row in csv.DictReader(f)]

    for row in provider_catalog:
        company = row['company']
        location = row['location']

        for place in location.replace('.', ',').split(','):
            place = place.strip()

            result[company][GEOCODE[place]] = 0

    return result


def get_country_code_from_ip(ip):
    """
    :param ip: the target ip we want to get country_code from
    :return: 2 digit country code or US +'_'+ 2 digit state code

    example:
    input: '192.169.128.0'
    output: 'US_AZ'
    """
    try:
        query_url = f'{IPWHOIS_URL}/{ip}?key={IPWHOIS_KEY}'

        response = requests.get(query_url)
        res_json = response.json()
        if not res_json.get('success', False):
            msg = res_json.get('message', 'Bad response from ipwhois api')
            raise Exception(msg)

        country_code = res_json['country_code']
        if country_code == 'US':
            state = US_STATE_ABBREVIATIONS[res_json['region']]
            country_code = f'US_{state}'

        return country_code

    except Exception:
        logger.warning(f'Could not get location for ip {ip}')
        return ""


def get_ip_sample(all_ip, n=SAMPLE_NUMBER):
    """
    :param all_ip: a list of ip addresses that we will sample from
    :param n: the length of the output sample list of ip addresses, n will be set to 200 if all_ip size is larger than 100000
    :return: a n-size list of ip addresses.

    example:
    input: all_ip = [0,2,3,...,99] n = 10
    output: [0, 10, 20, 30,...,90]
    """
    length = len(all_ip)
    if length > 100000:
        n = 200
    interval = int(length/n)
    if interval < 1:
        return all_ip
    ipsamples = []
    index = 0
    while len(ipsamples) < n:
        ipsamples.append(all_ip[index])
        index += interval

    return ipsamples


def process_no_country_code(cidr, company):
    """

    :param cidr: The cidr we read from the ip_ranges.csv. We will try to get state code and count ip number from this cidr
    :param company: The company that the cidr(s) belong to
    :return: We count the ip number in the cidr(s), sample 10 ip from the cidr to see whether they contain different location, if unified location,
    we only add it to the global counters, if not, we have to split the ip number according to sample ratio and add them to counters
    """
    if '-' in cidr:
        # handle the dash ip range issue by converting it to multi-cidr
        ip, ip_end = cidr.split('-', 1)
        ip = ip.strip()
        ip_end = ip_end.strip()
        ipnetworks = netaddr.iprange_to_cidrs(ip, ip_end)

        ips = [str(ip) for ipnetwork in ipnetworks for ip in ipnetwork]  # all the ip
        count = len(ips)

    else:
        # handle the multi-cidr issue by split(',')
        ipnetworks = [IPNetwork(c.strip()) for c in cidr.split(',')]

        ips = [str(ip) for ipnetwork in ipnetworks for ip in ipnetwork]  # all the ip
        count = len(ips)

    # sample from the all the ip to see if they belong to one region, if so, add on the counter, if not, move on to status-check
    ipsamples = get_ip_sample(ips, SAMPLE_NUMBER)

    country_code_samples = []
    for ipsample in ipsamples:
        country_code_samples.append(get_country_code_from_ip(ipsample))

    # empty code samples, continue to the next cidr
    if len(set(country_code_samples)) == 0:
        return

    # if the code samples only contain one type of country_code, then we only need to verify if it is non-empty,
    # and then count the ip number and add it to the global counter
    elif len(set(country_code_samples)) == 1:
        for country_code_sample in country_code_samples:
            if country_code_sample:
                counters[company][country_code_sample] += count
                for ipnetwork in ipnetworks:
                    cidr_to_geocode_map[company][str(ipnetwork)] = {country_code_sample: 1}
                break

    # Distribute the country_code samples by ratio
    else:
        country_code_samples = [country_code_sample for country_code_sample in country_code_samples if country_code_sample]  # delete empty code
        total = len(country_code_samples)
        ratio = Counter(country_code_samples)
        for country_code_sample, appearance in ratio.items():
            counters[company][country_code_sample] += count * appearance / total
        for ipnetwork in ipnetworks:
            cidr_to_geocode_map[company][str(ipnetwork)] = {country_code_sample: appearance / total
                                                            for country_code_sample, appearance in ratio.items()}


def process_us_country_code(cidr, company):
    """
    :param cidr: The cidr we read from the ip_ranges.csv. We will try to get state code and count ip number from this cidr
    :param company: The company that the cidr(s) belong to
    :return: We try to get the state code by the WHOIS service, if the cidr is private, then randomly assign a state code from the option and record.
    Finally, we add on the global counters the country_code(state code) and the ip number.
    """
    if '-' in cidr:
        # handle the dash ip range case
        ip_start, ip_end = cidr.split('-', 1)
        ip_start = ip_start.strip()
        ip_end = ip_end.strip()

        ipnetworks = netaddr.iprange_to_cidrs(ip_start, ip_end)
        count = sum([len(ipnetwork) for ipnetwork in ipnetworks])

    else:
        # handle the multi-cidr case by split(',')
        ipnetworks = [IPNetwork(c.strip()) for c in cidr.split(',')]
        count = sum([len(ipnetwork) for ipnetwork in ipnetworks])
        ip_start = ipnetworks[0][0]

    country_code = get_country_code_from_ip(ip_start)

    option = generate_location_option()

    # if the cidr is private, then randomly assign a state code from the option and record.
    if not country_code:
        country_code = random.choice([country_code_option for country_code_option in option[company] if 'US' in country_code_option])

        # Here we get a code without using ip, so we have to record this in a map called detect
        for ipnetwork in ipnetworks:
            detect[str(ipnetwork)] = country_code

    counters[company][country_code] += count
    for ipnetwork in ipnetworks:

        cidr_to_geocode_map[company][str(ipnetwork)] = {country_code: 1}


def process_non_us_country_code(cidr, company, country_code):
    """
    :param cidr: The cidr we read from the ip_ranges.csv. We will try to count ip number from this cidr
    :param company: The company that the cidr(s) belong to
    :param country_code: The native country_code in the ip_ranges.csv file
    :return: We count the ip number and then add it to the gloabl counters
    """
    if '-' in cidr:
        # handle the dash ip range case
        ip_start, ip_end = cidr.split('-', 1)
        ip_start = ip_start.strip()
        ip_end = ip_end.strip()

        ipnetworks = netaddr.iprange_to_cidrs(ip_start, ip_end)
        count = sum([len(ipnetwork) for ipnetwork in ipnetworks])

    else:
        # handle the multi-cidr case by split(',')
        ipnetworks = [IPNetwork(c.strip()) for c in cidr.split(',')]
        count = sum([len(ipnetwork) for ipnetwork in ipnetworks])

    counters[company][country_code] += count
    for ipnetwork in ipnetworks:
        cidr_to_geocode_map[company][str(ipnetwork)] = {country_code: 1}


def process_amazon():
    """
    :return: This function reads in the amazon_ip_ranges and count the country_code appearance by either SERVER_TO_REGION or ip detection and add it
    to the counter.
    """
    amazon_ip_ranges_path = 'input/amazon_ip_ranges.json'
    with open(amazon_ip_ranges_path, 'r') as f:
        prefixes = json.load(f)['prefixes']

    for i, prefix in enumerate(prefixes):
        cidr = prefix["ip_prefix"]

        server = prefix["region"]

        logger.info(f'Processing Amazon: {i} {cidr}')

        # Skip ipv 6 address
        if ':' in cidr:
            continue

        service_ip_addresses = IPNetwork(cidr)
        count = len(service_ip_addresses)

        # get code if not in SERVER_TO_REGION
        if server not in SERVER_TO_REGION['AWS']:
            ip = service_ip_addresses[0]

            country_code = get_country_code_from_ip(ip)
            logger.info(f'New found server at {country_code} for {cidr}')

            counters["Amazon"][country_code] += count
            cidr_to_geocode_map['Amazon'][cidr] = {country_code: 1}
        # get code from SERVER_TO_REGION and GEOCODE and record it to the detect map
        else:
            country_code = GEOCODE[SERVER_TO_REGION['AWS'][server].lower()]
            counters["Amazon"][country_code] += count
            cidr_to_geocode_map['Amazon'][cidr] = {country_code: 1}
            detect[cidr] = country_code  # record to detection map because we get the country_code by SERVER_TO_REGION methods, not ip.


def process_microsoft():
    """
    :return: This function reads in the microsoft_ip_ranges and count the country_code appearance by either SERVER_TO_REGION or ip detection and add it
    to the counter.
    """
    with open('input/microsoft_ip_ranges.json', 'r') as f:
        lines = json.load(f)['Region']

    # change the key of Azure SERVER_TO_REGION to the same format as it appears in the ip_ranges file. (West Europe 2 -> westeurope2)
    temp_dict = {}
    for server, region in SERVER_TO_REGION['Azure'].items():
        temp_dict[server.lower().replace(' ', '')] = region
    SERVER_TO_REGION['Azure'] = temp_dict

    for i, line in enumerate(lines):
        if 'IpRange' not in line:
            continue
        server = line["@Name"]
        cidrs = [iprange_data["@Subnet"] for iprange_data in line["IpRange"]]

        logger.info(f'Processing Microsoft: {i} {cidrs}')

        ipnetworks = [IPNetwork(cidr.strip()) for cidr in cidrs]
        count = sum([len(ipnetwork) for ipnetwork in ipnetworks])

        # server not in SERVER_TO_REGION, use first ip to detect region code
        if server not in SERVER_TO_REGION:
            ip = ipnetworks[0][0]
            country_code = get_country_code_from_ip(ip)
            logger.info(f'New found server at {country_code} for {cidrs}')
            counters["Microsoft"][country_code] += count
            for ipnetwork in ipnetworks:
                cidr_to_geocode_map['Microsoft'][str(ipnetwork)] = {country_code: 1}

        # get code from SERVER_TO_REGION and GEOCODE, and write the relationship in the detect map
        else:
            country_code = GEOCODE[SERVER_TO_REGION['Azure'][server].lower()]
            counters["Microsoft"][country_code] += count
            for ipnetwork in ipnetworks:
                cidr_to_geocode_map['Microsoft'][str(ipnetwork)] = {country_code: 1}
                detect[str(ipnetwork)] = country_code  # record to detection map because we get the country_code by SERVER_TO_REGION methods, not ip.


def write_json():
    """
    :return:This function takes the global counters and converts its value from appearance to frequency, and finally dump the counters and detect to
    json file
    """
    # convert appearance to frequency
    result = defaultdict(dict)
    for company, location_distribution in counters.items():
        total = sum(location_distribution.values())
        for country_code, appearance in location_distribution.items():
            result[company][country_code] = appearance / total

    with open('../../distribution/input/company_location_distribution.json', 'w') as f:
        json.dump(result, f)

    with open('output/detect.json', 'w') as f:
        json.dump(detect, f)

    with open('output/cidr_to_geocode_map.json', 'w') as f:
        json.dump(cidr_to_geocode_map, f)


if __name__ == '__main__':

    with open('input/ip_ranges.csv', 'r', encoding='utf-8-sig') as f:
        ip_ranges = [row for row in csv.DictReader(f)]

    for i, row in enumerate(ip_ranges):

        row_company = row['service_name'].strip()
        row_cidr = row['identifier']
        row_country_code = row['country']

        logger.info(f'Now at row {i}, {row_cidr}')

        # Skip ipv 6 address
        if ':' in row_cidr:
            continue

        # no region code, sampling to check if this cidr belongs to one location, if so, add to counter, else split it by sample ratio
        if not row_country_code:
            process_no_country_code(cidr=row_cidr, company=row_company)

        # With US code, assume all the ip addresses in this Cidr belong to one location, so only one request to check out state, then overwrite it
        elif row_country_code == 'US':
            process_us_country_code(cidr=row_cidr, company=row_company)

        # Easiest case, with non-US code. Count the length and add to the counter of that company
        else:
            process_non_us_country_code(cidr=row_cidr, company=row_company, country_code=row_country_code)

    process_amazon()

    process_microsoft()

    output()
