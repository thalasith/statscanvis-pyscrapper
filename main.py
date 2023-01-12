from flask import Flask
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import os
import library.data_pipeline as data_pipeline

app = Flask(__name__)

def monthly_employment_by_industry_job():
    pid="1410020101"
    table_name = "monthly_employment_by_industry"
    pick_members_1 = {"names": ["1.1", "1.2", "1.3", "1.4", "1.5", "1.6", "1.7", "1.8", "1.9", "1.10", "1.11", "1.12", "1.14", "1.15"], "values": ["Canada", "Newfoundland and Labrador", "Prince Edward Island", "Nova Scotia", "New Brunswick", "Quebec", "Ontario", "Manitoba", "Saskatchewan", "Alberta", "British Columbia", "Yukon", "Northwest Territories", "Nunavut"]}
    pick_members_2 = {"names": ["2.2", "2.3"], "values": ["Salaried employees paid a fixed salary", "Employees paid by the hour"]}
    
    pick_members_dict = {"&pickMembers%5B0%5D=": pick_members_1, "&pickMembers%5B1%5D=": pick_members_2}
    filter_names = {"pick_member_1": "Geography", "pick_member_2": "Type of employee"}

    data_pipeline.data_pipeline_job(pid=pid, table_name=table_name, pick_members_dict=pick_members_dict, filter_names=filter_names)

def monthly_salary_by_industry_job():
    pid="1410022301"
    table_name = "average_weekly_earnings_by_industry"
    pick_members_2 = {"names": ['2.2'], "values": ['average_weekly_earnings']}
    pick_members_3 = {"names": ['3.1', '3.2', '3.3', '3.4', '3.10', '3.17', '3.21', '3.34', '3.143', '3.144', '3.215', '3.253', '3.267', '3.284', '3.295', '3.306', '3.321', '3.331', '3.354', '3.367', '3.377', '3.393'],"values": ['industrial_aggregate_including_unclassified_businesses', 'industrial_aggregate_excluding_unclassified_businesses', 'goods_producing_industries', 'forestry_logging_and_support', 'mining_quarrying_and_oil_and_gas_extraction', 'utilities', 'construction', 'manufacturing', 'service_producing_industries', 'trade', 'transportation_and_warehousing', 'information_and_cultural_industries', 'finance_and_insurance', 'real_estate_and_rental_and_leasing', 'professional_scientific_and_technical_services', 'management_of_companies_and_enterprises', 'educational_services', 'health_care_and_social_assistance', 'arts_entertainment_and_recreation', 'accommodation_and_food_services', 'other_services_except_public_administration', 'public_administration'] }

    pick_members_dict = {"&pickMembers%5B0%5D=": pick_members_2, "&pickMembers%5B1%5D=": pick_members_3}
    filter_names = {"pick_member_1": "Estimate", "pick_member_2": "North American Industry Classification System (NAICS)"}
    transpose = True
    data_pipeline.data_pipeline_job(pid=pid, table_name=table_name, pick_members_dict=pick_members_dict, filter_names=filter_names, transpose=transpose)

test = "Hi there, welcome to my data pipeline!"
scheduler = BackgroundScheduler()

# schedule an apscheduler job to run monthly_employment_by_industry_job every last day of the month
job = scheduler.add_job(monthly_employment_by_industry_job, 'cron', day='last', hour=16, minute=00)
job = scheduler.add_job(monthly_salary_by_industry_job, 'cron', day='last', hour=16, minute=3)

for job in scheduler.get_jobs():
    job.modify(next_run_time=datetime.now())
scheduler.start()

@app.route("/")
def home():
    return "Hi there!"

if __name__ == '__main__':
    app.run(debug=True, port=os.getenv("PORT", default=5000))