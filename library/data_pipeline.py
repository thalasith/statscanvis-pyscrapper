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

def data_pipeline_job(pid, table_name, pick_members_dict, filter_names, transpose=False):
    today = date.today()
    four_months_ago = date.today() - relativedelta(months=+4)
    start_month=str(four_months_ago.month).zfill(2)
    start_year=str(four_months_ago.year)
    end_month=str(today.month).zfill(2)
    end_year=str(today.year)

    referencePeriods = start_year + start_month + "01" + "%2C" + end_year + end_month + "28" 
    keys = list(pick_members_dict.keys())
    values = list((pick_members_dict[key]["names"] for key in keys))

    filter_names_keys = list(filter_names.keys())
    filter_names_values = list(filter_names.values())
    pick_members_1_dict = dict(zip(pick_members_dict["&pickMembers%5B0%5D="]["names"], pick_members_dict["&pickMembers%5B0%5D="]["values"]))
    pick_members_2_dict = dict(zip(pick_members_dict["&pickMembers%5B1%5D="]["names"], pick_members_dict["&pickMembers%5B1%5D="]["values"]))

    if transpose:
        final_df = pd.DataFrame()
    result = [(x, y) for x in values[0] for y in values[1]]

    if pick_members_dict == {}:
        url = 'https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=' + pid + '&cubeTimeFrame.startMonth='+ start_month + '&cubeTimeFrame.startYear=' + start_year + '&cubeTimeFrame.endMonth=' + end_month +'&cubeTimeFrame.endYear=' + end_year + '&referencePeriods=' + referencePeriods
        df = scrapper.simple_scrapper(url, filter_names).iloc[-1]
        latest_month = df["month"]
        
        query = "SELECT * FROM " + table_name +  " WHERE geography = '" + pick_member_1 + "' AND type_of_employee = '" + pick_member_2 + "' AND month = '" + latest_month + "';"

    else:
        for x, y in result:
            pick_member_1=pick_members_1_dict[x]
            pick_member_2=pick_members_2_dict[y]
            url = 'https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=' + pid + '&pickMembers%5B0%5D='+ x + '&pickMembers%5B1%5D='+ y + '&cubeTimeFrame.startMonth='+ start_month + '&cubeTimeFrame.startYear=' + start_year + '&cubeTimeFrame.endMonth=' + end_month +'&cubeTimeFrame.endYear=' + end_year + '&referencePeriods=' + referencePeriods
            df = scrapper.simple_scrapper(url, filter_names.values()).iloc[-1]
            latest_month = df["month"]

            if transpose:
                new_df = pd.DataFrame(df).T.melt(['date', 'month'], var_name='province', value_name=pick_member_2)
                provinces = ['canada', 'newfoundland_and_labrador', 'prince_edward_island', 'nova_scotia', 'new_brunswick', 'quebec', 'ontario', 'manitoba', 'saskatchewan', 'alberta', 'british_columbia', 'yukon', 'northwest_territories', 'nunavut']
                new_df = new_df[new_df['province'].isin(provinces)]
                if final_df.empty:
                    final_df["month"] = new_df["month"]
                    final_df["date"] = new_df["date"]
                    final_df["province"] = final_df["province"].str.replace("_", " ").str.title()

                final_df[pick_member_2] = new_df[pick_member_2]
            
            if transpose == False:
                query = "SELECT * FROM " + table_name +  " WHERE geography = '" + pick_member_1 + "' AND type_of_employee = '" + pick_member_2 + "' AND month = '" + latest_month + "';"
                upload_sql(query, df, table_name, latest_month, pick_member_1, pick_member_2, x, y)
        
        if transpose == True:
            query = "SELECT * FROM " + table_name + " WHERE month = '" + latest_month + "';"
            upload_sql(query, final_df, table_name, latest_month, pick_member_1, pick_member_2, x, y)
        
    print(bold_green +"Data pipeline job completed for " + table_name + " table at " + str(today) + reset)

def upload_sql(query, data, table_name, latest_month, pick_member_1, pick_member_2, x, y):
    conn_string = 'mysql+pymysql://' + os.environ["USERNAME"] + ':' + os.environ["PASSWORD"] + '@' + os.environ["HOST"] + '/' + os.environ["DATABASE"] 
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
            print(latest_month + " " + pick_member_1 + " "+ pick_member_2 + " data already exists in " + table_name + " table")
        else:
            data.to_sql(table_name, engine, if_exists="append", index=False)
            print("Inserted data from " + x + " and " + y + " into " + table_name + " successfully")
