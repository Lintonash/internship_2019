import requests
import lxml.html
import os
import re
from datetime import datetime
import csv
import json
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULT_PATH = 'gcp_outage.csv'

REQUEST_HEADERS = {
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'Content-Type': 'application/x-www-form-urlencoded',
    'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36'
                   ' (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'),
    'Accept': ('text/html,application/xhtml+xml,'
               'application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8'),
    'Accept-Language': 'en-US,en;q=0.8,he;q=0.6',
}

links = [
# ('https://status.cloud.google.com/incident/', 'appengine'),
# ('https://status.cloud.google.com/incident/', 'compute'),
('https://status.cloud.google.com/incident/', 'storage'),
('https://status.cloud.google.com/incident/', 'bigquery'),
# ('https://status.cloud.google.com/incident/', 'cloud-ddatastore'),
# ('https://status.cloud.google.com/incident/', 'cloud-dev-tools'),
# ('https://status.cloud.google.com/incident/', 'cloud-functions'),
# ('https://status.cloud.google.com/incident/', 'cloud-iam'),
# ('https://status.cloud.google.com/incident/', 'cloud-ml'),
# ('https://status.cloud.google.com/incident/', 'cloud-networking'),
# ('https://status.cloud.google.com/incident/', 'cloud-pubsub'),
# ('https://status.cloud.google.com/incident/', 'cloud-sql'),
# ('https://status.cloud.google.com/incident/', 'cloud-dataflow'),
# ('https://status.cloud.google.com/incident/', 'container-engine'),
# ('https://status.cloud.google.com/incident/', 'developers-console'),
# ('https://status.cloud.google.com/incident/', 'google-stackdriver'),
# ('https://status.cloud.google.com/incident/', 'support')
]

def write_csv(data):
    with open(RESULT_PATH, 'a') as out:
        csv_out = csv.writer(out)

        for line in data:
            csv_out.writerow(line)


def main():
    if not os.path.exists(RESULT_PATH):
        write_csv([['title', 'impact', 'start_time', 'end_time' 'service', 'geography']])

    events = []

    ZONES = ['asia-east1', 'asia-east2', 'asia-northeast1', 'asia-south1', 'asia-southeast1', 'australia-southeast1', 'europe-north1', 'europe-west1', 'europe-west2', 'europe-west3', 'europe-west4', 'europe-west6', 'northamerica-northeast1', 'southamerica-east1', 'us-central1', 'us-east1', 'us-east4', 'us-west1', 'us-west2']
    years = ['15', '16', '17', '18', '19']
    issues = ['000', '001', '002', '003', '004', '005', '006', '007', '008', '009', '010', '011', '012', '013', '014', '015', '016', '017', '018', '019', '020', '021', '022', '023', '024', '025', '026', '027', '028', '029', '030', '031', '032', '033', '034', '035', '036', '037', '038', '039', '040', '041', '042', '043', '044', '045', '046', '047', '048', '049', '050', '051', '052', '053', '054', '055', '056', '057', '058', '059', '060', '061', '062', '063', '064', '065', '066', '067', '068', '069', '070', '071', '072', '073', '074', '075', '076', '077', '078', '079', '080', '081', '082', '083', '084', '085', '086', '087', '088', '089', '090', '091', '092', '093', '094', '095', '096', '097', '098', '099']

    for link in links:
        for year in years:
            for issue in issues:
                time.sleep(2)
                title = link[1] + ' ' + str(year) + str(issue)
                parsed_link = link[0] + link[1] + '/' +  year + issue
                bubbles = set()

                html = requests.get(parsed_link, headers=REQUEST_HEADERS)

                if not html.ok:
                    continue

                doc = lxml.html.fromstring(html.content)

                try:
                    data = doc.xpath('//*[@id="maia-main"]/table')
                    for i in data[0]:
                        for j in i.getiterator():
                            if j.values() and 'bubble' in j.values()[0] and j.values()[0] != 'bubble ok':
                                bubbles.add(j.values()[0])

                    bubbles = list(bubbles)
                    bubbles = [x.replace('bubble', '').strip() for x in bubbles]

                    data2 = doc.xpath('//*[@id="maia-main"]/div[2]/p/strong')
                    start = data2[0].text
                    end = data2[1].text

                    data3 = doc.xpath('//*[@id="maia-main"]/div[1]/p')
                    second_title = data3[0].text
                    zone = re.findall(f"{'|'.join(ZONES)}", second_title)

                    print([title, bubbles, start, end, link[1], zone])
                    events.append([title, bubbles, start, end, link[1], zone])

                except Exception as e:
                    print(f'error in {parsed_link}', e)
                    continue

    write_csv(events)

main()
