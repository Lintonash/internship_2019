import pandas as pd
import sqlite3 as db
import json
from collections import Counter, defaultdict
import numpy as np
import scipy.stats as st
import matplotlib.pyplot as plt
import sys
import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s', datefmt='%Y%m%d %H:%M:%S')
logger = logging.getLogger()

with open('input/provider_name_keyword.json', 'r') as f_keyword:
    CONV = json.load(f_keyword)

DURATION_THRESHOLD = 15  # Duration threshold
DB_PATH = 'dB/event_catalog.db'  # Path of db file
TAG = 'services-linton2'

INDICATORS = ["INFRASTRUCTURE_SERVICE_HOSTING",
              "INFRASTRUCTURE_SERVICE_EMAIL",
              "INFRASTRUCTURE_SERVICE_DNS",
              "INFRASTRUCTURE_SERVICE_CRM",
              "INFRASTRUCTURE_SERVICE_CONTENT_DELIVERY_NETWORK"]


class Distribution:
    def __init__(self):
        self.dist_names = {"simple": ["norm", "lognorm", "expon", "pareto"],
                           "advance": ["norm", "lognorm", "expon", "pareto", "exponweib", "weibull_max", "weibull_min", "genextreme"]}
        self.dist_results = []
        self.params = {}

        self.distribution_name = ""
        self.pvalue = 0
        self.param = None

        self.is_fitted = False

    def fit(self, y, dist_type):
        """
        This function receive a list of number (y) and a switch (dist_type) between 'simple' and 'advance', which is used to select which set of
        distributions this function will try to fit (y) to. Finally, it returns the best fit (distribution_name) and the distribution parameters
        """
        self.dist_results = []
        self.params = {}
        for dist_name in self.dist_names[dist_type]:
            dist = getattr(st, dist_name)
            param = dist.fit(y)
            self.params[dist_name] = param
            # Applying the Kolmogorov-Smirnov test
            _, p = st.kstest(y, dist_name, args=param)
            self.dist_results.append((dist_name, p))

        # select the best fitted distribution
        sel_dist, p = (max(self.dist_results, key=lambda item: item[1]))
        # store the name of the best fit and its p value
        self.distribution_name = sel_dist
        self.pvalue = p

        self.is_fitted = True
        return self.distribution_name, self.params[self.distribution_name]

    def plot_distribution(self):
        """
        This function only works after the object calls the distribution.fit function. It will plot the pdf of the best fitted distribution.
        """
        param = self.params[self.distribution_name]
        dist = getattr(st, self.distribution_name)
        x = np.linspace(dist.ppf(0.01, *param[:-2], loc=param[-2], scale=param[-1]), dist.ppf(0.99, *param[:-2], loc=param[-2], scale=param[-1]), 100)
        plt.plot(x, dist.pdf(x, *param[:-2], loc=param[-2], scale=param[-1]), 'r-', lw=5, alpha=0.6)
        plt.show()


def read_data(path=DB_PATH):
    """
    This function takes a path of the .db file (DB_PATH) and read in the db file, filter out all items with duration less than a threshold and return
    a dataframe
    """
    conn = db.connect(path)
    data = pd.read_sql("select * from event_catalog", conn)
    data = data[data['duration'] >= DURATION_THRESHOLD]  # filter out all rows with duration less than 15 minutes
    return data


def conv_name(target_name, indic):
    """
    :param target_name: a string of a service provider name
    :param indic: a string of indicator which represents the provider type.
    :return: if the provider name contains some keyword of a known provider in the indicated provider type, the function return the known provider's
    name. If provider name contains no keyword, return itself.

    example
    input: "Amazon Cloud Platform", "INFRASTRUCTURE_SERVICE_HOSTING"
    output: "AWS"
    """
    for service, names in CONV[indic].items():
        for name in names:
            if name in target_name.lower():
                return service
    return target_name


def read_gbq_data(tag):
    """
    :param tag: a string of tag name that we want to get data with.
    :return: After doing gbq, the function parse the data into dictionary and returns it
    """
    data = {ind: [] for ind in INDICATORS}
    projectid = 'sme-modeling'

    for indicator in INDICATORS:
        logger.info(f'Reading data from big query, indicator: {indicator}')
        query = f"""
        select json_extract(result, '$.risks_data.{indicator}.data') as res
        from `sme_modeling.ModuleResult`
        where '{tag}' in unnest(tags)
        and module = 'droid'
        and result like '%{indicator}%'
        """

        data_frame = pd.read_gbq(query, projectid, dialect='standard')

        for row in data_frame['res'].tolist():
            if row and row != '{}':
                data[indicator].append(json.loads(row))

    logger.info('Big query done')
    return data


def generate_continuous_dist(data, variable_of_interest, dist_type='simple', plot_flag='False'):
    """
    :param data: a data frame which contains our variable of interest
    :param variable_of_interest: a string of variable name that we care about and want to get distribution of
    :param dist_type: a string that switch between 'simple' and 'advance', deciding which set of distributions the fit function will try.
    :param plot_flag: a boolean to decide whether to plot the pdf or not
    :return: If the variable_of_interest name cannot be found in the dataframe, return nothing and end the function. Otherwise the function with
    generates a json file showing the best fitted distribution name and parameters.
    """
    if variable_of_interest not in data.columns:
        logger.warning(f"Invalid variable of interest!")
        return

    dst = Distribution()

    sequence = data[variable_of_interest].dropna().tolist()
    name, parameters = dst.fit(sequence, dist_type=dist_type)
    if plot_flag:
        dst.plot_distribution()
    result = {"distribution": name, "parameters": parameters}

    with open(f'output/{variable_of_interest}_distribution.json', 'w') as f:
        json.dump(result, f)


def generate_categorical_dist(data, variable_of_interest):
    """
    :param data: a data frame which contains our variable of interest
    :param variable_of_interest: a string of variable name that we care about and want to get distribution of
    :return: this functions generates a file showing the frequency of each category
    """
    categorical_sequence = data[variable_of_interest].dropna().tolist()
    result = {category: appearance / len(categorical_sequence) for category, appearance in Counter(categorical_sequence).items()}

    with open(f'output/{variable_of_interest}_distribution.json', 'w') as f:
        json.dump(result, f)


def provider_counter(ass_data):
    """
    :param ass_data: a dictionary of the assessment data that we get from GBQ
    :return: Inside each provider type, we extract the detected service names row by row, convert them into known names, create a counter for them
    """
    counters = {}

    # generates counters for each provider type
    for indicator in INDICATORS:
        service_list = []
        for row in ass_data[indicator]:
            if 'detected_services' not in row:
                logger.warning(f'Data format error in {row}')
                continue
            for detected_service in row['detected_services']:
                service_list.append(conv_name(detected_service, indicator))

        counters[indicator] = Counter(service_list)

    return counters


def generate_type_dist(provider_catalog, tag):
    """
    :param provider_catalog: The provider catalog that contains providers that we care about
    :param tag: tag is a string that specify the assessment data that we will retrieve
    :return: We read in assessment data from GBQ, from this data, we calculate the distribution between different provider types. Then we filter out
    providers that we don't care about, calculte the distribution inside each provider type, save it to a json file.
    """

    ass_data = read_gbq_data(tag=tag)  # pull assessment data and store it in the dictionary called 'ass_data'

    ass_counters = provider_counter(ass_data=ass_data)

    providers = defaultdict(list)

    for _, row in provider_catalog.iterrows():
        providers[row['type']].append(row['provider'])  # For each type, create a list of providers that we care about (on the provider google sheet)

    filtered_ass_counters = {indicator: {} for indicator in INDICATORS}
    # only keep providers that we care about
    for indicator, counter in ass_counters.items():
        for service, appearance in counter.items():
            if service in providers[indicator]:
                filtered_ass_counters[indicator][service] = appearance

    # count total providers number from ass_counter
    total_for_each_indicator = {}
    for indicator, counter in ass_counters.items():
        total_for_each_indicator[indicator] = sum(counter.values())
    total_for_all_indicator = sum(total_for_each_indicator.values())

    result = {indicator: {"provider": {}} for indicator in INDICATORS}
    # convert appearance to fraction
    # for each indicator, convert the number inside "probability", convert the number of every provider inside "provider".
    for indicator, counter in filtered_ass_counters.items():
        total_for_all_filtered_provider = sum(counter.values())
        result[indicator] = {"probability": total_for_each_indicator[indicator] / total_for_all_indicator,
                             "provider": {service: appearance / total_for_all_filtered_provider for service, appearance in counter.items()}}

    with open('output/provider_distribution.json', 'w') as f:
        json.dump(result, f)


def generate_location_dist(provider_catalog, company_location_distribution):
    """

    :param provider_catalog: Providers that we care about
    :param company_location_distribution: Location distribution for each company. This file is generated by cidr_to_geocode in util
    :return: After doing a coupling between provider and company, save the location distribution to json file.
    """
    result = defaultdict(dict)

    for _, row in provider_catalog.iterrows():
        # For each provider, we use their company_location_distribution as this provider's location_distribution
        row_provider = row['provider']
        row_company = row['company']
        row_type = row['type']
        if row_company in company_location_distribution:
            result[row_type][row_provider] = company_location_distribution[row_company]
        else:
            logger.warning(f'Currently there is no data for {row_company} : {row_provider}')

    with open('output/location_distribution.json', 'w') as f:
        json.dump(result, f)


if __name__ == '__main__':
    df = read_data(DB_PATH)  # read in scraping data
    generate_continuous_dist(data=df, variable_of_interest='duration', dist_type='simple', plot_flag=False)
    generate_continuous_dist(data=df, variable_of_interest='affect_rate', dist_type='simple', plot_flag=False)
    generate_categorical_dist(data=df, variable_of_interest='impact')

    provider_catalog_data = pd.read_csv('input/provider_catalog.csv')  # read in provider catalog data
    generate_type_dist(provider_catalog=provider_catalog_data, tag=TAG)

    with open('input/company_location_distribution.json', 'r') as f_comp_loc_dist:
        company_location_distribution_data = json.load(f_comp_loc_dist)
    generate_location_dist(provider_catalog=provider_catalog_data, company_location_distribution=company_location_distribution_data)
