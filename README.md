
# Introduction
Today, 1% of the world is a barely livable hot zone.By 2070, that portion could go up to 19%.Billions of people call this land home.
Where will they go? we will help the countries and governments to understand the bisc relation between the climate change and the immigration issue. In this project we will use  some important datasets related to immigration process and the temperature with the world. We will use some usful data enginnering tools like data werehouses,AWS  S3,  Spark, Apache Airflow and AWS Warehouse like Redshif to deal with those datasets and try to indicates to the concerned people how the weather change and temperature is a important factor to the immigration process. 

The project follows the follow steps:

<li><a href="#s1">Step 1: Scope the Project and Gather Data</a>
<li><a href="#s2">Step 2: Explore and Assess the Data</a>
<li><a href="#s3">Step 3: Define the Data Model</a>
<li><a href="#s4">Step 4: Run ETL to Model the Data</a>
<li><a href="#s5">Step 5: Complete Project Write Up</a>

# Datasets
- 1- I94 Immigration dataset: This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace.  [This](https://www.trade.gov/national-travel-and-tourism-office) is where the data comes from. 

- 2- World Temperature dataset: This dataset came from Kaggle. You can read more about it [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).
    
    
# Steps 
To help guide the project, we've broken it down into a series of steps.
    
<h3>Step 1: Scope the Project and Gather Data</h3><a id="s1"></a>
Since the scope of the project will be highly dependent on the data, these two things happen simultaneously. In this step, we’ll:
Identify and gather the data we'll be using for your project (at least two sources and more than 1 million rows).
Explain what end use cases we'd like to prepare the data for (e.g., analytics table, app back-end, source-of-truth database, etc.)
    

## <h3> Step 2: Explore and Assess the Data </h3><a id="s2"></a>
Explore the data to identify data quality issues, like missing values, duplicate data, etc.
Document steps necessary to clean the data
    
## <h3>Step 3: Define the Data Model</h3><a id="s3"></a>
Map out the conceptual data model and explain why we chose that model
List the steps necessary to pipeline the data into the chosen data model
    
## <h3>Step 4: Run ETL to Model the Data</h3><a id="s4"></a>
Create the data pipelines and the data model
Include a data dictionary
Run data quality checks to ensure the pipeline ran as expected
Integrity constraints on the relational database (e.g., unique key, data type, etc.)
Unit tests for the scripts to ensure they are doing the right thing
Source/count checks to ensure completeness
    
## <h3> Step 5: Complete Project Write Up  </h3><a id="s5"></a>
What's the goal? What queries will we want to run? How would Spark or Airflow be incorporated? Why did we choose that model?
Clearly state the rationale for the choice of tools and technologies for the project.
Document the steps of the process.
Propose how often the data should be updated and why.
Include a description of how you would approach the problem differently under the following scenarios:
If the data was increased by 100x.
If the pipelines were run on a daily basis by 7am.
If the database needed to be accessed by 100+ people.    
    
