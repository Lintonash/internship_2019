import pandas as pd
import sqlite3 as db
import os


DUR_THRES = 15  # Duration threshold
SCRAPE_PATH = '../scrape/data/'  # Path of all the scraped data

COLUMN_ORDER = ['issue', 'provider_type', 'provider', 'service', 'location', 'duration', 'affect_rate', 'impact', 'cause', 'intensity', 'time']

EVENT_CATALOG_TABLE = 'event_catalog'

NEW_EVENT_TABLE = 'new_event'


def read_scrape_data(path=SCRAPE_PATH):
    """
    :param path: Path of all the json file, notice that under this directory there should be only json files.
    :return: A dataframe containing all the data
    """
    json_files = [pos_json for pos_json in os.listdir(path)]

    frames = []  # list to put all the data frame together
    for json_file in json_files:
        frames.append(pd.read_json(os.path.join(path, json_file), 'records'))
    frames = pd.concat(frames, sort=True)
    frames = frames[frames['duration'] >= DUR_THRES]
    return frames


def list_to_string(scrape_data):
    """
    :param scrape_data: Here data means the dataframe that read_scrape_data() returns
    :return: a new dataframe that all the lists are converted into strings, separating elements with ','. We do this because sqlite3 does not support
    array
    """
    new_data = scrape_data
    list_cols = ['provider_type', 'service', 'location']
    for col in list_cols:
        new_col = []
        for cell in scrape_data[col]:
            if type(cell) == list:
                new_col.append(", ".join(cell))
            else:
                new_col.append(cell)
        new_data[col] = new_col
    return new_data


def sql_command_create_table(table_name):
    """
    :param table_name: A string of the table name that we want to create
    :return: A string of sql command that we can execute
    """
    sql_command = f"""CREATE TABLE IF NOT EXISTS {table_name}(
                       issue text PRIMARY KEY,
                       provider_type text,
                       provider text,
                       service text,
                       location text,
                       duration integer,
                       affect_rate real,
                       impact text,
                       cause text,
                       intensity text,
                       time text
                       )"""
    return sql_command


def sql_command_insert_to_table(to_table_name, from_table_name):
    """
    :param to_table_name: The name of the table that receive everything. Should contain historical data and should NEVER BE OVERWRITTEN UPON
    :param from_table_name: The name of the table that we use to save the data of a new scraping round
    :return: A string of sql command that we will execute
    """
    sql_command = f"""INSERT INTO {to_table_name}
                    SELECT * 
                    FROM {from_table_name}
                    WHERE NOT EXISTS (
                        SELECT issue FROM {to_table_name}
                        WHERE {to_table_name}.issue = {from_table_name}.issue
                    )"""
    return sql_command


if __name__ == "__main__":
    df = read_scrape_data(SCRAPE_PATH)
    df = list_to_string(df)  # convert all the list data in dataframe to strings
    conn = db.connect('db/event_catalog.db')
    c = conn.cursor()

    # Create two tables if not exist, event_catalog is for later use, temp_event is to store the updated json file
    c.execute(sql_command_create_table(EVENT_CATALOG_TABLE))
    c.execute(sql_command_create_table(NEW_EVENT_TABLE))
    conn.commit()

    # specify column name and order
    df = df[COLUMN_ORDER]

    # store updated json file to the temp_event table
    df.to_sql(NEW_EVENT_TABLE, conn, if_exists='replace', index=False)

    # compare two table, insert non-existed value from temp_event to event_catalog
    c.execute(sql_command_insert_to_table(EVENT_CATALOG_TABLE, NEW_EVENT_TABLE))
    conn.commit()
