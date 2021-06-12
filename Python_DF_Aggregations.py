
######## Load DF from blob storage ##############
df = spark.read.format("csv") \
.options(header='true', delimiter = ',') \
.schema(customSchema) \
.load("/mnt/files/Employee.csv")

