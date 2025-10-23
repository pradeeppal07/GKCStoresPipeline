from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType, DoubleType
import configparser
# Initializing Spark Session
Spark = SparkSession.builder.appName("DataIngestAndRefine").master("local").getOrCreate()

# Read the Configs
config = configparser.ConfigParser()
config.read(r'../projectconfigs/config.ini')
inputLocation = config.get('paths', 'inputLocation')

# schema making
landingFileSchema = StructType([
    StructField('Sale_ID', StringType(), True),
    StructField('Product_ID', StringType(), True),
    StructField('Quantity_Sold', IntegerType(), True),
    StructField('Vendor_ID', StringType(), True),
    StructField('Sale_Date', TimestampType(), True),
    StructField('Sale_Amount', DoubleType(), True),
    StructField('Sale_Currency', IntegerType(), True)
])

landingFileDF = Spark.read\
    .schema(landingFileSchema)\
    .option("delimiter", "|")\
    .csv(inputLocation)

landingFileDF.show()
