# COMMAND ----------
######## Load DF from blob storage ##############
df = spark.read.format("csv") \
.options(header='true', delimiter = ',') \
.schema(customSchema) \
.load("/mnt/files/Employee.csv")

# COMMAND ----------
#max of a column
from pyspark.sql.functions import *
df.select(max("Department_id")).show()


# COMMAND ----------
#min, avg, mean of a col
df.select(min("Department_id")).show()
df.select(avg("Department_id")).show()
df.select(mean("Department_id")).show()

# COMMAND ----------
#show count
df.select(count("Department_id")).show()