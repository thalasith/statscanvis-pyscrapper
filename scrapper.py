import pandas as pd
import requests
import json
from bs4 import BeautifulSoup

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
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    result = find_between(soup.prettify(), 'tableContainerElement = $(".tableContainer").clone();', 'window.addEventListener("resize", function() {') + 'end'
    data = find_between(result, 'prepareTable(', '\n')[:-2]
    json_data = json.loads(data)
    rows = json_data['rows']

    # We need to transform the data into a format that is easier to work with.


    new_rows = []
    for row in rows:
        values = row['values']
        for value in values:
            new_rows.append(value["value"])

    # Return the headers for the data table.
    headers = next(item for item in json_data['headers']["columnHeaders"] if item["name"] == "Reference period")
    header_values = []
    for item in headers["values"]:
            header_values.append(item["value"])
    rows_values = {}
    key = ""
    data = []
    index = 0
    for row in new_rows:
        temp_data = {}
        if not isfloat(row):
            key = row
            rows_values[key] = []
            data = []
            index = 0
        if isfloat(row):
            data.append(float(row))
            rows_values[key] = data
            index += 1

    test_data = []
    keys = []
    for key, value in rows_values.items():
        index=0
        temp_data = {}
        temp_data["key"] = key
        key = key.replace(" ", "_").replace(",","").replace("(","").replace(")","").replace("-","_").replace("__","_").lower()[:60]
        keys.append(key)
        for i in value:
            temp_data[header_values[index]] = i
            index += 1
        test_data.append(temp_data)

    df = pd.DataFrame(test_data).transpose().drop("key")
    df.columns = keys
    df["date"] = soup.find_all('meta', attrs={'name': 'dcterms.issued'})[0]['content']

    df['month'] = df.index
    df.reset_index(drop=True, inplace=True)

    for filter_name in filter_names:
        new_name = filter_name.replace(" ", "_").replace(",","").replace("(","").replace(")","").replace("-","_").replace("__","_").lower()[:60]
        df[new_name] = next(item for item in json_data['headers']["columnHeaders"] if item["name"] == filter_name)["values"][0]["value"]
    
    return df

    # # Connecting to Planet Scale
    # ssl_args = {'ssl_ca': "/etc/ssl/cert.pem"}

    # conn_string = 'mysql+pymysql://' + os.getenv("USERNAME") + ':' + os.getenv("PASSWORD") + '@' + os.getenv("HOST") + '/' + os.getenv("DATABASE") 

    # engine = create_engine(conn_string, connect_args=ssl_args)


    # with engine.begin() as engine:
    #     df.to_sql('statscan', engine, if_exists='replace', index=False)
    #     print("Data inserted into database")

