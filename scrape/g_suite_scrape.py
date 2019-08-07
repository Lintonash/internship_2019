import tweepy
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
import json
from datetime import datetime
import sys
import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s', datefmt='%y%m%d %H:%M:%S')
logger = logging.getLogger()

RESULT_PATH = './data/g_suite_outage.json'

ckey = 'LtungrMUcekG71cNImr4T3kUY'
csecret = 'zmnlrQMHjVehSglgdsjSXSi6qmTAzSo9J1uLtryHJcYrwXbZaq'
atoken = '1138695520828960769-2F6ebYsCyXXzPP1CGHGeqdIBQeiaq3'
asecret = 'n1itAP1STH5A8gtutlBspBKK3ztosNLpMh1ITnMIbmQ0f'
ACCOUNT_NAME = 'g_suite_status'

PROVIDER_TYPE = 'INFRASTRUCTURE_SERVICE_EMAIL'
PROVIDER = "G_Suite"

events = {}


def get_twitter_link():
    """
    :return: Go to twitter API, find the account 'g_suite_status', retrieve the links to all the incident pages.
    """
    auth = tweepy.OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)
    api = tweepy.API(auth)
    cursor = tweepy.Cursor(api.user_timeline, screen_name=ACCOUNT_NAME)
    links = []
    logger.info('Collecing links...')
    for tweet in cursor.items():
        try:
            links.append(tweet._json['entities']['urls'][0]['expanded_url'])
        except IndexError:
            logger.exception(f'No link found in tweet')
            continue
    logger.info(" Links collection done. Opening Chrome...")
    return links


def get_service(driver):
    result_service = driver.find_element_by_xpath('//*[@id="aad-content"]/div/h2').text
    result_service = result_service.replace(' - Service Details', '').strip()

    return result_service


def get_impact(driver):
    # bubble is a round icon on the incident page that indicates how severe the incident was.
    bubbles = []
    for bubble in driver.find_elements_by_xpath('//*[@id="aad-content"]/div/div[3]/div/table/tbody/tr/td/span'):
        bubbles.append(bubble.get_attribute('class'))
    result = None
    if "aad-red-circle" in bubbles:
        result = 'outage'
    elif "aad-yellow-circle" in bubbles:
        result = 'degradation'
    else:
        logger.warning(f'Unclear impact with {bubbles}')
    return result


def get_duration_and_starttime(driver):
    times = []
    for time in driver.find_elements_by_xpath('//*[@id="aad-content"]/div/div[3]/div/table/tbody/tr/td[2]/div'):
        times.append(time.text)

    end = datetime.strptime(times[0], "%m/%d/%y, %I:%M %p")
    start = datetime.strptime(times[-1], "%m/%d/%y, %I:%M %p")
    result = int((end - start).total_seconds() / 60)
    return result, datetime.strftime(start, '%Y-%m-%d %H:%M')


def get_location(driver):
    descriptions = ''
    for description in driver.find_elements_by_xpath('//*[@id="aad-content"]/div/div[3]/div/table/tbody/tr/td[3]/div'):
        descriptions += description.text
    # There is very limited location information for G_suite, for now there are only "USA" or "eastern USA"
    if "eastern USA" in descriptions:
        result = "Eastern USA"
    elif "USA" in descriptions:
        result = "USA"
    else:
        result = None
    return result


def write_json():
    """
    :return: write the global events to json file
    """
    result = []
    for issue, dic in events.items():
        dic["issue"] = issue
        result.append(dic)

    with open(RESULT_PATH, 'w') as f:
        json.dump(result, f)

    logger.info(f'Done storing. File at: {RESULT_PATH}')


def chrome_driver_scrape(links):
    driver = webdriver.Chrome()
    wait = WebDriverWait(driver, 5)
    for link in links:
        logger.info(f'Now scraping {link}')
        driver.get(link)
        try:
            wait.until(ec.visibility_of_element_located((By.XPATH, '//*[@id="aad-content"]/div/div[3]/div/table/tbody')))
        except Exception:
            logger.exception(f'No table on {link}')
            break

        issue = link.split('iid=')[1]  # Get the hash code and use it as a unique name of the incident
        service = get_service(driver)

        # service should be a specific service name. If we try to access a very old event that exceed the time limit of google, we will be redirect to
        # the dash board page, then we know we don't need to go any further
        if service == "G Suite Status Dashboard":
            logger.info('Reach out-dated incident')
            break

        impact = get_impact(driver)

        duration, starttime = get_duration_and_starttime(driver)

        location = get_location(driver)

        affect_rate = 0.5
        cause = None

        if issue not in events:
            # new event, create a new row
            events[issue] = {"provider_type": PROVIDER_TYPE, "provider": PROVIDER, "service": [service], "location": location, "duration": duration,
                             "affect_rate": affect_rate, "impact": impact, "cause": cause, "intensity": None, "time": starttime}
        else:
            # existed event, use longest duration as the duration of the event, append non-record service name
            if duration > events[issue]["duration"]:
                events[issue]["duration"] = duration
            if service not in events[issue]["service"]:
                events[issue]["service"].append(service)

    driver.close()
    logger.info('Done scraping.')


def scrape():
    twitter_links = get_twitter_link()
    chrome_driver_scrape(links=twitter_links)
    write_json()
