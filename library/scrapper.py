import pandas as pd
import requests
import json
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import itertools
import os
from dotenv import load_dotenv
load_dotenv()

# Helper function to find data between two strings
def find_between( s, first, last ):
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        return s[start:end]
    except ValueError:
        return ""

# Helper function to check if a string is a float
def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False

def simple_scrapper(url, filter_names):
    # Loading and parsing the data from the StatsCan website.
    # Data is presented as a jquery function that is called to generate the table. We parse out the relevant code and return back the data.
    #The data as of 2021-03-01 is between a script tag with the following id: "tableContainerElement = $(".tableContainer").clone();" 
    # and "window.addEventListener("resize", function() {"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    result = find_between(soup.prettify(), 'tableContainerElement = $(".tableContainer").clone();', 'window.addEventListener("resize", function() {') + 'end'
    data = find_between(result, 'prepareTable(', '\n')[:-2]
    json_data = json.loads(data)

    # Return the headers for the data table.
    # The headers contain the data for our columns. We need to extract the values from the headers and return them as a list.
    headers = next(item for item in json_data['headers']["columnHeaders"] if item["name"] == "Reference period")
    header_values = []
    for item in headers["values"]:
            header_values.append(item["value"])
    

    # We need to transform the data into a format that is easier to work with.
    # The data is presented as a list of dictionaries. Each dictionary contains a list of dictionaries.
    # We need to flatten the data into a list of dictionaries and afterwards return a list of the values from the dictionary.
    rows = json_data['rows']
    flattened_rows = list(itertools.chain.from_iterable([row['values'] for row in rows]))
    new_rows = []
    for row in flattened_rows:
        new_rows.append(row['value'])
    
    # Because the new rows are now a list with mixed data types, we need to separate the data into a dictionary.
    keys = []
    data = {}

    for row in new_rows:
        if not isfloat(row):
            key = row.replace(" ", "_").replace(",","").replace("(","").replace(")","").replace("-","_").replace("__","_").lower()[:60]
            keys.append(key)
            data[key] = []
        if isfloat(row):
            data[key].append(float(row))

    rows_values = {key: value for key, value in data.items()}
    # We transform the data to its final format and return it as a pandas dataframe.
    final_data = [{"key": name, **{month: value for month, value in zip(header_values, values)}} for name, values in rows_values.items()]
 
    df = pd.DataFrame(final_data).transpose().drop("key")
    df.columns = keys
    df["date"] = soup.find_all('meta', attrs={'name': 'dcterms.issued'})[0]['content']

    for filter_name in filter_names:
        new_name = filter_name.replace(" ", "_").replace(",","").replace("(","").replace(")","").replace("-","_").replace("__","_").lower()[:60]
        df[new_name] = next(item for item in json_data['headers']["columnHeaders"] if item["name"] == filter_name)["values"][0]["value"]
    
    df['month'] = df.index
    df.reset_index(drop=True, inplace=True)
    
    return df

def drop_table(table_list):
    # Connecting to Planet Scale
    ssl_args = {'ssl_ca': "/etc/ssl/cert.pem"}

    conn_string = 'mysql+pymysql://' + os.getenv("USERNAME") + ':' + os.getenv("PASSWORD") + '@' + os.getenv("HOST") + '/' + os.getenv("DATABASE") 

    table_name = ["monthly_employment_by_industry"]

    for table in table_name:
        engine = create_engine(conn_string, connect_args=ssl_args)
        drop_table_query = 'DROP TABLE ' + table + ';'
        # print(drop_table_query)
        with engine.connect() as con:
            con.execute(drop_table_query)
        print("Successfully dropped table " + table)

