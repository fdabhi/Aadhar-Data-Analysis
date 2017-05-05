from pyspark.sql import SparkSession
from pyspark.sql import Row
import collections

# Create a SparkSession (Note, the config section is only for Windows!)
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/tmp").appName("AadharDetails").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(EnrolmentAgency=str(fields[1]), State=str(fields[2]), Age=int(fields[7]), Gender=str(fields[6]), Aadhaar=int(fields[8]), Reject=int(fields[9]))

lines = spark.sparkContext.textFile("UIDAI-ENR-DETAIL-20170504.csv")
#Exclude hearder
linesHeader = lines.filter(lambda l:"Enrolment Agency" in l)
linesNoHeader = lines.subtract(linesHeader)

#Convert to Rows
aadhar = linesNoHeader.map(mapper)

# Infer the schema, and register the DataFrame as a table.
schemaAadhar = spark.createDataFrame(aadhar).cache()
schemaAadhar.createOrReplaceTempView("aadhar")

# SQL can be run over DataFrames that have been registered as a table.
states = spark.sql("SELECT State,SUM(Aadhaar) as count FROM aadhar group By State order by count DESC")

states.repartition(1).write.csv("ApprovedByStates", sep=',')

agency = spark.sql("SELECT EnrolmentAgency,SUM(Aadhaar) as count FROM aadhar group By EnrolmentAgency order by count DESC")

agency.repartition(1).write.csv("ApprovedByAgency", sep=',')

rejects = spark.sql("SELECT State,SUM(Reject) as count FROM aadhar group By State order by count DESC")

rejects.repartition(1).write.csv("RejectsByStates", sep=',')

statesGender = spark.sql("SELECT State,count(CASE WHEN Gender='M' THEN 1 END) as countMale,count(CASE WHEN Gender='F' THEN 1 END) as countFemale FROM aadhar group By State order by countMale DESC,countFemale DESC")

statesGender.repartition(1).write.csv("GenderByStates", sep=',')


spark.stop()
