# GitHub Events Analysis Pipeline

by: Crystal McEnhimer

## Project Overview
This project analyzes the commit history, contributor activity, and development trends of popular Python data science repositories on Github. The analysis processed GitHub archival data to provide insights into package popularity, contributor behavior, and development activity.trends.

## Data Source
I worked with the GitHub Events Archive (https://www.gharchive.org), which provides historical data of all public GitHub activity. This project focuses on events from 10 popular Python packages, namely:

*pandas, numpy, matplotlib, scikit-learn, scipy, pytorch, tensorflow, plotly, seaborn, polars*

The dataset consisted of various types of events, including push, watch, issues, pull requests, forks, and repository creation.

## Challenges and Solutions
My initial plan was to use GitHub's standard API to gather my data, but I quickly ran into rate limiting issues that prevented me from collecting the volume of data I needed in a timely manner. To overcome this, I switched to using the GitHub Events Archive, which provided a significantly larger dataset in a shorter period of time. I also faced challenges in downloading and processing such a large number of events. In order to speed up the handling of data, I implemented parallel processing techniques to process multiple files simultaneously.

#### Tools Used:
- Data Ingestion: Python requests 
- Data Processing and Storage: DuckDB
- Workflow Orchestration: Prefect
- Containerization: Docker


## Key Insights
1. **Most Active Repository**: [scipy] was the package with the most commits in our dataset
2. **Most Popular Commit Type**: [PushEvent] is the most popular event type among all repositories
3. **Busiest Times/Days**: Most commits happen on [Monday], with a peak around [15:00 UTC], which aligns with typical work hours in the US and Europe.
4. **Top Packages Close in Popularity**: Among the top 15 most popular packages, the top 10 are extremely close in number of commits. There's less than a 1000 commit difference between #1 and #10. From #10 to #11, however, the number of commits drops off from almost 15,000 to less than 3,000 commits.


![image](visualizations/event_type_distribution.png)


## GitHub Repository Link
https://github.com/vcx4ka/crm-dp3-repo

## How to Run
1. Install dependencies:
    pip install -r requirements.txt
2. Run the pipeline with prefect:
    python orchestration/pipeline.py