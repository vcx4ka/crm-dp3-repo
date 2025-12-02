# Team GitHub Events Analysis

## Team members
Crystal McEnhimer

## Data Source
I worked with the GitHub Events Archive (https://www.gharchive.org), which provides historical data of all public GitHub activity. This project focuses on events from 10 popular Python packages (pandas, numpy, matplotlib, scikit-learn, scipy, pytorch, tensorflow, plotly, seaborn, polars). This project analyzes the commit history, contributor activity, and development trends of the repositories for these packages. The analysis processed GitHub archival data to provide insights into package popularity, contributor behavior, and development activity trends.

## Challenges and Solutions
My initial plan was to use GitHub's standard API to gather my data, but I quickly ran into rate limiting issues that prevented me from collecting the volume of data I needed in a timely manner. To overcome this, I switched to using the GitHub Events Archive, which provided a significantly larger dataset in a shorter period of time. I also faced challenges in downloading and processing such a large number of events. In order to speed up the handling of data, I implemented parallel processing techniques to process multiple files simultaneously. I utilized python requests for data ingestion, DuckDB for data processing and storage, prefect for workflow orchestration, and docker for containerization.

## Key Insights
Scipy was the package with the most commits in our dataset, making it the most active repository we were analyzing. The most popular event type across all repositories was the PushEvent. These repositories were the busiest on Mondays, with a large peak in activity around 15:00 UTC, coinciding with typical work hours in the US and Europe. Among the top 15 most popular. Among the top 15 most popular packages, the top 10 are extremely close in number of commits. There's less than a 1000 commit difference between #1 and #10. From #10 to #11, however, the number of commits drops off from almost 15,000 to less than 3,000 commits.

## Plot/Visualization

![image](visualizations/event_type_distribution.png)


## GitHub Repository
https://github.com/vcx4ka/crm-dp3-repo
