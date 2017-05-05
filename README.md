# Aadhar-Data-Analysis
Analyzing a dataset from Aadhaar - a unique identity issued to all resident Indians using SparkSQL in Python

##Dependencies

* Spark 2.0: (http://spark.apache.org/)
* Python 2.7: (https://www.python.org/)

##Usage

I choose Aadhaar Dataset which is available at Aadhaar public data [portal](https://data.uidai.gov.in/uiddatacatalog/getDatsetInfo.do?dataset=UIDAI-ENR-DETAIL) using SparkSQL in Python to query below questions.

##Queries

1. Count the number of cards approved by States.
2. Count the number of cards approved by Enrolment Agency.
3. Count the number of cards rejected by States.
4. Count the number of Aadhaar applicants by gender split by States. 

```
spark-submit AadharAnalysis.py
```
