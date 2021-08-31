# JobAdAnalytics

## General Info
This project makes use of the free to use common craw datasets housed on Amazon Web Services to fulfill client questions
related to job market advertisements
## Technologies
* Scala 2.11.12
* SBT Build Tool 1.5.5
* Hadoop
* Spark
* SparkSQL
* Hive
* Intellij Community
* Amazon Web Services

## Setup
Set Amazon Web Service keys within generics.properties in JobAdAnalytics/src/main/resources/dev/
  * Now, to setup your access credentials:
  * Create a file named `app.config` in your project root folder.
  * Add your AWS access key and secret as such:
``` 
AWS_ACCESS_KEY_ID=INSERT_YOUR_ACCESS_KEY
AWS_SECRET_ACCESS_KEY=INSERT_YOUR_ACCESS_SECRET
```
  * Save the file.
