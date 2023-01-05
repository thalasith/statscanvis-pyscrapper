import pandas as pd
import requests
import re
import json
from bs4 import BeautifulSoup

def find_between( s, first, last ):
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        return s[start:end]
    except ValueError:
        return ""
def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False

if __name__ == '__main__':
    url = 'https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1410020101&pickMembers%5B0%5D=1.3&pickMembers%5B1%5D=2.1&cubeTimeFrame.startMonth=06&cubeTimeFrame.startYear=2022&cubeTimeFrame.endMonth=10&cubeTimeFrame.endYear=2022&referencePeriods=20220601%2C20221001'
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    result = find_between(soup.prettify(), 'tableContainerElement = $(".tableContainer").clone();', 'window.addEventListener("resize", function() {') + 'end'
    data = find_between(result, 'prepareTable(', '\n')[:-2]
    json_data = json.loads(data)
    rows = json_data['rows']

    new_rows = []
    for row in rows:
        values = row['values']
        for value in values:
            new_rows.append(value["value"])
    category = next(item for item in json_data['headers']["columnHeaders"] if item["name"] == "Geography")
    category_selected = category["values"][0]["value"]
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
        key = key.replace(" ", "_").replace(",","").replace("(","").replace(")","").replace("-","_").replace("__","_").lower()
        keys.append(key)
        for i in value:
            temp_data[header_values[index]] = i
            index += 1
        test_data.append(temp_data)

    df = pd.DataFrame(test_data).transpose().drop("key")
    df.columns = keys
    df["category"] = category_selected
    df["date"] = soup.find_all('meta', attrs={'name': 'dcterms.issued'})[0]['content']
    print(df.head())
