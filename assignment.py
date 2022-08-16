from pyspark.sql import *
from pyspark import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

#starting the session
spark = SparkSession.builder.master('local[3]').getOrCreate()

#data given 
data = [('Washing Machine', '1648770933000', 20000, 'Samsung', 'India', '0001'),
               ('Refrigerator', '1648770999000', 35000, ' LG', 'null', '0002'),
               ('Air Cooler', '1648770948000', 45000, ' Voltas', 'null', '0003')
               ]

#schema for dataframe
schema = StructType([
    StructField("Product_Name", StringType(), True),
    StructField("Issue_Date", StringType(), True ),
    StructField("Price", IntegerType(), True),
    StructField("Brand", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Product_Number", StringType(), True)   
])


#creating dataframe
df=spark.createDataFrame(data,["Product_Name","Issue_Date", "Price", "Brand", "Country", "Product_Number"])

#all values 
df.show()

#convert miliseconds to timestamp from 1st to 10th pos
df_new = df.withColumn("timestamp", from_unixtime(substring(col("Issue_Date"), 1, 10), "yyyy-MM-dd'T'HH:mm:ss[.SSS][ZZZ]"))
df_new.show(truncate=False)

#get timestamp 
df_new = df_new.withColumn('Date_Extract', col('timestamp').cast('date'))
df_new.show(truncate=False)

#removing the space from the column brand
df_new = df_new.withColumn('Brand', ltrim(df.Brand))
df_new.show(truncate=False)


#removed null and replaced it with empty val
df_emp = df_new.withColumn('Country', regexp_replace('Country', 'null', ''))
df_emp.show(truncate=False)



##############################################################################
#2nd part

#given dataset 
data2 = [(150711, 123456, 'EN', '456789', '2021-12-27T08:20:29.842+0000', '0001'),
             (150439, 234567, 'UK', '345678', '2021-01-28T08:21:14.645+0000', '0002'),
             (150647, 345678, 'ES', '234567', '2021-12-27T08:22:42.445+0000', '0003')
             ]


df2  = spark.createDataFrame(data2, ["Source_Id" , "Transaction_Number" , "Language"	, "Model_Number" ,"Start_Time","Product_Number"])

#snake case conversion of df2 columns
df2_low =  df2.toDF(*[c.lower() for c in df2.columns])

#conversion of miliseconds into date timestamp
df2_low = df2_low.withColumn('miliseconds', concat(unix_timestamp(to_date(date_format('start_time',"yyyy-MM-dd HH:mm:ss.SSS"))), substring('start_time',21,3)))

#show 
df2_low.show(truncate=False)

#combined dataframes both 1st and 2nd on product_number 
combined_df = df_emp.join(df2_low, df_emp.Product_Number == df2_low.product_number, "fullouter")

#show
combined_df.show(truncate=False)

#language as english 
combined_df.filter(df2_low.language=='EN').show(truncate=False)