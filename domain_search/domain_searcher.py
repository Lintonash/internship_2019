import csv
import json
from collections import defaultdict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.keys import Keys
from collections import OrderedDict
import time
from cleanco import cleanco  # This module to handle void words in company name is an old one
import random
import string
import sys
import logging
from urllib.parse import urlparse

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s', datefmt='%y%m%d %H:%M:%S')
logger = logging.getLogger()

UNWANT_DOMAIN_KEYWORDS = ['wiki', 'baidu', 'bloomberg', 'linkedin', 'facebook', 'twitter', 'yellow-page', 'yellowpage', 'zoominfo', 'crunchbase',
                          'youtube', 'whitepage', 'instagram', 'hoovers', 'glassdoor', 'kompass', 'amazon', 'manta', 'pinterest', 'yelp', 'spokeo',
                          'nasdaq', 'reuter']

GOOGLE = 'https://www.google.com'

TIME_TO_HAVE_A_BREAK = 50  # After doing a Google search for this many times, have a long break
BROWSER = "firefox"  # Fill in 'chrome' or 'firefox', which are the only two that we support
PAUSE_FACTOR = 1  # let PAUSE_FACTOR = x, which means between every search, pause for a time ranging from x to 2x seconds


class DomainSearcher:
    def __init__(self, browser, file_name, chinese=False):
        self.input_path = f'input/{file_name}'
        self.output_path = f'output/{file_name}'
        self.companies_name = []  # List to store all the companies name

        self.search_counter = 0  # How many searches we have done
        self.fail_counter = defaultdict(int)  # A counter of fail_cites, to find more third party websites

        self.result = OrderedDict()  # Dictionary to store result
        self.chinese = chinese  # True if the companies name is in Chinese
        self.browser = browser

        if browser == 'chrome':
            chrome_options = webdriver.ChromeOptions()
            # load in the proxy.zip in order to automatically input username and password, if we want to change proxy, please modify the zip file
            chrome_options.add_extension("proxy.zip")

            # set language to English, the country code after 'en_' must be the same with the proxy
            chrome_options.add_experimental_option('prefs', {'intl.accept_languages': 'en,en_UK'})
            self.driver = webdriver.Chrome(chrome_options=chrome_options)
        elif browser == 'firefox':
            self.driver = webdriver.Firefox()  # if we are using firefox, we give up proxy

        # Declare wait time for this driver
        self.wait = WebDriverWait(self.driver, 5)

    @staticmethod
    def cross_validation(title, company_name):
        """
        :param title: The string of the title of the first result that google returns
        :param company_name: The company name that we use as a searching keyword
        :return: True if title and keyword share one or more words, False otherwise.
        """
        company_name = company_name.translate(str.maketrans(string.punctuation, ' '*len(string.punctuation)))  # Replace all punctuation with space
        company_name_set = set(cleanco(company_name).clean_name().lower().split(' '))

        title = title.translate(str.maketrans(string.punctuation, ' '*len(string.punctuation)))   # Replace all punctuation with space
        title_set = set(cleanco(title).clean_name().lower().split(' '))

        return len(company_name_set & title_set)

    @staticmethod
    def check_if_element_exist(html, by, value):
        """
        :param html: The html object that we want to check
        :param by: selenium find element By what
        :param value: The string that you want selenium to find for you
        :return: True if there is such an element on the page, False otherwise
        """
        try:
            html.find_element(by, value)
        except Exception:
            return False
        return True

    def get_domain(self, company_google_result, company_name):
        """
        :param company_google_result: The result of google search, it is a html element object
        :param company_name: The company name that we search google for
        :return: First, a domain; Second, a list of domains that fail to pass the validation; Third, if it passed the validation, what is the number
        of overlapped words.

        example:
        input: "Nike"
        output: "www.nike.com", ["www.stockx.com", "www.adidas.com", "www.finishline.com"], 1

        input: "a very small company"
        output: "", ["www.abc.com", www.google.com", ...], 0
        """
        domain_candidates = []
        fail_cites = []  # The list to append inaccurate domain, if all cites are inaccurate, we write this list to the data for manual check
        # Check the knowledge panel, which is the most reliable. In this case, number of overlapped words is set to 10
        if self.check_if_element_exist(company_google_result, By.XPATH, '//*[@id="rhs_block"]/div/div[1]/div/div[1]/div[2]/div[2]/div/a/span[2]'):
            return company_google_result.find_element(By.XPATH,
                                                      '//*[@id="rhs_block"]/div/div[1]/div/div[1]/div[2]/div[2]/div/a/span[2]').text, fail_cites, 10

        # Check the button "Website" that is less reliable but acceptable, need to do cross validation
        elif self.check_if_element_exist(company_google_result, By.XPATH, '//*[@class="ab_button"][text()="Website"]'):
            website = company_google_result.find_element(By.XPATH, '//*[@class="ab_button"][text()="Website"]').get_attribute('href')
            domain_candidates.append(website)

        # If Google shows that a company has a parent organization, add it to the company_name for the validation
        if self.check_if_element_exist(company_google_result, By.XPATH,
                                       '//*[@id="rhs_block"]/div/div[1]/div/div[1]/div[2]/div[2]/div/div[4]/div/div/span[2]/a'):
            parent_company_name = company_google_result.find_element_by_xpath(
                '//*[@id="rhs_block"]/div/div[1]/div/div[1]/div[2]/div[2]/div/div[4]/div/div/span[2]/a').text
            company_name = f'{company_name} {parent_company_name}'

        # Check the cites from searching result
        if not self.check_if_element_exist(company_google_result, By.ID, 'rso'):
            logger.warning(f'Error when searching for {company_name}')
            return None, fail_cites, 0

        rso = company_google_result.find_element_by_xpath('//*[@id="rso"]')  # rso means Google's searching result item. By locating rso, we skip ads
        cites = (rso.find_elements_by_xpath('//*[contains(local-name(), "cite")]')[0:5])  # cite is the url that Google find for us, we take the first 5

        # sometimes cite will be in this format 'www.google.com › Israel › Tel Aviv, so get rid of this '›'
        cites = [cite.text.split('›')[0] for cite in cites]
        # put on "http://" so that urllib.parser can recognize it
        cites = [cite if 'https://' in cite else f'https://{cite}' for cite in cites]

        domain_candidates.extend(cites)

        for domain_candidate in domain_candidates:
            # if any unwanted keyword appears in a cite, continue to the next one
            if any(unwant_word in domain_candidate for unwant_word in UNWANT_DOMAIN_KEYWORDS):
                continue

            domain = urlparse(domain_candidate).netloc
            # Do cross_validation, no need to do cross_validation if the company name is in Chinese.
            if self.chinese:
                return domain, fail_cites, 10  # No validation, no number of overlapped words
            domain_google_result = self.google_search(domain)
            title = domain_google_result.find_element_by_xpath('//*[@id="rso"]').find_element_by_tag_name('h3').text
            overlapped_word_num = self.cross_validation(title, company_name)
            if overlapped_word_num > 0:
                return domain, fail_cites, overlapped_word_num
            # If a url fail to pass the cross validation, add the domain to the tail of fails_cites
            fail_cites.append(f'{domain}: {title}')

        logger.warning(f"No candidate passes validation: {company_name}")
        domain = ''

        # Count the fail_cites, so maybe we can find more third party websites that need to be put on the unwanted list
        for fail_cite in fail_cites:
            self.fail_counter[fail_cite] += 1
        return domain, fail_cites, 0  # number of overlapped word is zero

    @staticmethod
    def get_english_name(company_google_result, company_name):
        """
        :param company_google_result: The google searching result for the company, it is a html element object
        :param company_name: This function is exclusively for the Chinese company
        :return: The English name of that comapny

        example:
        input: "安踏体育"
        output: "Anta Sports"
        """
        try:
            return company_google_result.find_element_by_xpath(
                '//*[@id="rhs_block"]/div/div[1]/div/div[1]/div[2]/div[1]/div[2]/div[1]/div/div/div[2]/div[1]/span').text
        except Exception:
            logger.warning(f"Couldn't find english name for {company_name}")
            return ''

    def google_search(self, keyword):
        """
        :param keyword: Locate the searching box and type in the keyword and press return
        :return: It will lead us to that searching result page, return nothing
        """
        self.search_counter += 1
        if self.search_counter == TIME_TO_HAVE_A_BREAK:
            self.search_counter = 0
            logger.info('Having a break')
            time.sleep(random.uniform(4 * PAUSE_FACTOR, 5 * PAUSE_FACTOR))  # long pause

        search = self.driver.find_element_by_name('q')
        search.clear()
        search.send_keys(keyword)
        search.send_keys(Keys.RETURN)
        time.sleep(random.uniform(PAUSE_FACTOR, 2 * PAUSE_FACTOR))
        try:
            self.wait.until(ec.visibility_of_element_located((By.XPATH, '//*[@id="rso"]')))
        except Exception:
            logger.exception(f'Searching error in {keyword}')
        return self.driver.find_element_by_xpath('/html')

    def get_domain_from_google(self, start_row=0, csv_file=True, json_file=False):
        """
        :param start_row: the number of row that we want the program to search from, by default we search for all, that means search from row 0
        :param csv_file: Bool, write csv or not
        :param json_file: Bool, write json or not
        :return: The DomainSearcher will read in from the file name, and go to www.google.com to search for all the companies name
        """

        # Read in the companies names
        with open(self.input_path, 'r', encoding='utf-8-sig') as f:
            self.companies_name = [row['name'] for row in csv.DictReader(f)]

        self.driver.get(GOOGLE)
        google = self.driver.find_element_by_xpath('/html')
        # If proxy is not used and this script is run in Israel, the page is in Hebrew. Need to switch to English version
        if self.check_if_element_exist(google, By.LINK_TEXT, "English"):
            self.driver.find_element_by_link_text('English').click()

        for i, company_name in enumerate(self.companies_name):
            if i < start_row:
                continue
            logger.info(i)
            company_google_result = self.google_search(keyword=company_name)

            domain, fail_cites, score = self.get_domain(company_google_result, company_name)  # score means overlapped words number

            # Save the result to a dictionary, which will deal with duplicate company_name
            if self.chinese:
                company_english_name = self.get_english_name(company_google_result, company_name)
                self.result[company_name] = {"domain": domain, "company_english_name": company_english_name, "fail_cites": fail_cites}
            else:
                self.result[company_name] = {"domain": domain, "score": score, "fail_cites": fail_cites}

            # Write result, by default we only write csv, not json.
            if csv_file:
                self.write_dictionary_to_csv()
            if json_file:
                self.write_dictionary_to_json()

        self.driver.close()

    def write_dictionary_to_csv(self):
        """
        :return: Write the self.result dictionary to a csv file
        """
        write_buffers = []
        for company_name, company_data in self.result.items():
            curr_buff = {}
            curr_buff['company_name'] = company_name
            curr_buff.update({key: ',\n'.join(value) if key == 'fail_cites' else value for key, value in company_data.items()})
            write_buffers.append(curr_buff)

        with open(self.output_path, 'w', encoding='utf-8-sig') as f:
            fieldnames = [fieldname for fieldname in write_buffers[0].keys()]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(write_buffers)

    def write_dictionary_to_json(self):
        """
        :return: Write the self.result dictionary to a json file
        """
        with open(self.output_path.replace('csv', 'json'), 'w') as f:
            json.dump(self.result, f)


a = DomainSearcher(BROWSER, 'WRe_Cyber_Test_Portfolios_for_Kovrr.csv')
a.get_domain_from_google(1642)
print(a.fail_counter)

