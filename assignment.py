from pyspark.sql import *
from pyspark import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[3]').getOrCreate()

#data given 
data= [("Washing Machine", 1648770933000    , 20000     ," Samsung"    ,"India",1),
("Refrigerator", 1648770999000 , 35000  ,  " LG",    "null"    ,2),
("Air Cooler" , 1648770948000 , 45000   , " Voltas"    , "null",3)]

#creating dataframe
df=spark.createDataFrame(data,["Product_Name","Issue_Date", "Price", "Brand", "Country", "Product_Number"])

#all values 
#df.show()

#convert miliseconds to timestamp
df_new = df.withColumn("timestamp", F.to_utc_timestamp(F.from_unixtime(F.col('Issue_Date')/1000,'yyyy-MM-dd HH:mm:ss'),'EST'))

#get timestamp 
df_new = df_new.withColumn('date_extract', col('timestamp').cast('date'))

#removing the space from the column brand
df_new = df_new.withColumn('Brand', ltrim(df.Brand))

#removed null and replaced it with empty val
df_emp = df_new.withColumn('Country', regexp_replace('Country', 'null', ''))

df_emp.show()

##############################################################################

data2 =[
    (150711 , 123456 ,"EN" , 456789 , "2021-12-27T08:20:29.842+0000" ,1),
    (150439	,234567	, "UK" , 345678 , "2021-12-27T08:21:14.645+0000" ,2),
     (150647	, 345678	, "ES"	, 234567, "2021-12-27T08:22:42.445+0000" ,3) ] 

df1  = spark.createDataFrame(data2, ["Source_Id" , "TransactionNumber" , "Language"	, "Model_Number" ,"Start_Time","Product_Number"])

#snake case conversion
df2_low =  df1.toDF(*[c.lower() for c in df1.columns])

#combined join i
combined_df = df1.join(df, df1.Product_Number == df.Product_Number, "inner")

#combined_df show
combined_df.show(truncate=False)
