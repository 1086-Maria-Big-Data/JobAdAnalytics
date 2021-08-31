<div align="center">
  <h1>Job Ad Analytics Project</h1>
</div>


## General Info
This project makes use of the free to use common craw datasets housed on Amazon Web Services to fulfill client questions
related to job market advertisements
## Description
Job Ad Analytics makes use of common crawl data (https://commoncrawl.org/the-data/get-started/) as a data source for analyizing job ads across the internet. Comman crawl is a universally accessible and analyzable repository of web crawl data that includes raw web page data, extracted metadata, and text extractions. The project's ability to handle the vast amounts of data provided by common crawl is achieved by running the spark application on AWS-EMR. Job Ad Analytics makes use of this data by querying the common crawl index for URLs containing job/s and career/s in order to filter out any non-job advertisments within each of the crawls. The program then looks through each segment within a crawl to access the WARC files which contain WARC objects that hold the data and metadata for every webpage we identified as an advertisemnt from the index. From there, each web page pertaining to job advertisements is analyzed and queried to fulfill the client's requirements.
## Features/Clilent Requirements
* Entry Level Experience
  * What percentage of entry level tech jobs require previous experience?
* Job Posting Spikes
  * Is there a significant spike in tech job postings at the end of business quarters?
  * If so, which quarter spikes the most?
* Largest Job Seekers
  * What are the three companies posting the most tech job ads?
* Largest Job Posting Trends
  * Is there a general trend in tech job postings over the past year?
  * What about the past month?
* Percent of Relatively Infrequent Job Seekers
  * What percent of tech job posters post no more than three job ads a month?
## Technologies
* Scala 2.11.12
* SBT Build Tool 1.5.5
* Hadoop 2.7.3
* Spark 2.2.0
* SparkSQL 2.2.0
* Intellij Community
* Amazon Web Services
  * EMR
  * S3
## Setup
Set Amazon Web Service keys within generics.properties in a dev folder
  * Now, to setup your access credentials:
  * Create a file named generics.properties JobAdAnalytics/src/main/rescources/dev/.
  * Add your AWS access key and secret as such:
``` 
AWS_ACCESS_KEY_ID=INSERT_YOUR_ACCESS_KEY
AWS_SECRET_ACCESS_KEY=INSERT_YOUR_ACCESS_SECRET
```
  * Save the file.
## Common Crawl Data Information

<div align="center">
  <h4>File hierarchy within each common crawl crawl</h4>
  <img src="/GitHubPictures/CommonCrawlHierarchyMedium.png" width="1000">
</div>


<div align="center">
  <h4>File types within each common crawl crawl</h4>
  <img src="/GitHubPictures/CommonCrawlFileStructure.png" width="800">
</div>

