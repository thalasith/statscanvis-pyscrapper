from datetime import date
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
load_dotenv()
import pandas as pd
try:
    from library import scrapper
except:
    import scrapper as scrapper

bold_green = "\x1b[32;1m"
reset = "\x1b[0m"

def data_pipeline_job(pid, table_name, pick_members_1, pick_members_2,filter_names) :
    today = date.today()
    four_months_ago = date.today() - relativedelta(months=+4)
    start_month=str(four_months_ago.month).zfill(2)
    start_year=str(four_months_ago.year)
    end_month=str(today.month).zfill(2)
    end_year=str(today.year)

    referencePeriods = start_year + start_month + "01" + "%2C" + end_year + end_month + "28" 
    pick_members_1_dict = dict(zip(pick_members_1["names"], pick_members_1["values"]))
    pick_members_2_dict = dict(zip(pick_members_2["names"], pick_members_2["values"]))

    result = [(x, y) for x in pick_members_1["names"] for y in pick_members_2["names"]]
    
    conn_string = 'mysql+pymysql://' + os.environ["USERNAME"] + ':' + os.environ["PASSWORD"] + '@' + os.environ["HOST"] + '/' + os.environ["DATABASE"] 

    for x, y in result:
        geography=pick_members_1_dict[x]
        type_of_employee=pick_members_2_dict[y]
        url = 'https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=' + pid + '&pickMembers%5B0%5D='+ x + '&pickMembers%5B1%5D='+ y + '&cubeTimeFrame.startMonth='+ start_month + '&cubeTimeFrame.startYear=' + start_year + '&cubeTimeFrame.endMonth=' + end_month +'&cubeTimeFrame.endYear=' + end_year + '&referencePeriods=' + referencePeriods
        df = scrapper.simple_scrapper(url, filter_names).iloc[-1]
        latest_month = df["month"]
        
        query = "SELECT * FROM " + table_name +  " WHERE geography = '" + geography + "' AND type_of_employee = '" + type_of_employee + "' AND month = '" + latest_month + "';"

        engine = create_engine(conn_string, connect_args={
        "ssl": {
            "ssl_ca": "ca.pem",
            "ssl_cert": "client-cert.pem",
            "ssl_key": "client-key.pem"
        }
    })
        with engine.begin() as engine:
            sql_data = pd.read_sql_query(query, engine)
            sql_latest_month = sql_data["month"].values[0]
            
            if (sql_latest_month == latest_month):
                print(latest_month + " " + geography + " "+ type_of_employee + " data already exists in " + table_name + " table")
            else:
                df.to_sql(table_name, engine, if_exists="append", index=False)
                print("Inserted data from " + x + " and " + y + " into " + table_name + " successfully")
    
    print(bold_green +"Data pipeline job completed for " + table_name + " table at " + str(today) + reset)
