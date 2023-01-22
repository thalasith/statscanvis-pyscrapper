<h1> Stats Canada Visualization Scrapper</h1>

The data tables in Stats Canada has a lot of useful data that gets updated on a regular basis - from the cost on specific goods to the salary in specific industries. The data is unfortunately isolated, and (relatively) manual labor intensive if one wants to visualize the data.

This project will help alleviate those burdens by building data pipelines that get regularly updated and the second part is to build visualization tools via a frontend. This library is focused on the first part of this project and is split into two components.

<h2> Components to this Library <h2>

<h3> Notebooks for Data Exploration, initializing the data, and building data pipelines. </h4>

Stats Canada provide separate tables of data to the public. In order to build a datapipeline, we need to first analyze these tables to build a library to scrape this data.

Each table will have three notebooks:

<ul> Data Exploration </ul>
<ul> Data Initialization </ul>
<ul> Data Pipelines </ul>

These represent the three different parts to our data pipeline. With each part, we will iterate the library used to automate the webscraping to make it more robust.

<h4> Data Exploration </h4>

The goal of this notebook is to explore ways to transform the data table on the Stats Canada website into a standardize way. This will make it easier to connect the data tables together for our visualization. A usecase example of why this is important is to have visualization charts for both the employment numbers and the average weekly salary by industry one page. This currently is not possible unless you do a lot of manual work to put the data in python / excel.

We use this notebook to explore the tables Stats Canada provide, see what filters are available to us, recognize any tiny irregularities in the data, fix said irregularities and transform the data into a pandas dataframe.

One tiny gotcha I found is that the filter names for the employment numbers by industry and the average weekly salary by industry had the same filter category of employee type. However, the spelling was different in both. One was "Type of Employees" while the other was "Type of Employee". This required some fixing to ensure that it had standardized spelling.

Once we have done the data exploration, we make any tweaks needed in the "Scrapper" library to ensure it is as robust as possible.

<h4> Data Initialization </h4>

The Stats Canada table currently provides multiple filters on their data sets. Depending on the table, these can range from the province we're interest in, male vs female, hourly employee vs salaried emplouyees and etc. Once the data exploration is complete and the scrapper library is updated, we use the scrapper library to iterate through all the different combinations of the filters and initialize our data tables in MySQL. This ensures our visualization tables have enough data.

We scrape all data available in 2022 as an initialization step.

<h4> Data Pipeline <h4>

Once we have initialized the data, we need to keep the data up to date. In this notebook, we build a data pipeline that checks the latest data in the MYSQL database vs the latest data that is provided by Stats Canada.

If the latest Stats Canada data is already in the MYSQL database, the pipeline will ignore the data. If the contrary, the pipeline will update the MYSQL database.

We make any updates here in the data_pipeline library and will be used in our other component of this project - scheduling the data pipeline.

<h3> Data Pipeline </h4>

In this component, we build a small Flask app that is pushed to a server and runs these jobs to ensure the data is up to date.

The frequency of the update depends on the frequency of the data table being updated by Stats Canada. Example is when the data table is updated on a monthly basis, we will update the table at the end of each month.

Note that each time we update the flask app, we force the app to run through each job first to ensure that the pipeline is running correctly.

<h2> Conclusions </h2>

Once we have built up enough data, we move over to the frontend component. In this component, we take the data provided and build dashboards to help visualize the data and point out any irregularities or patterns in the data.
