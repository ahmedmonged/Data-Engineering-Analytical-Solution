# Databricks notebook source
#  Mount an Azure Blob Storage container onto the Databricks filesystem
dbutils.fs.mount(
  source = "wasbs://destination@otrainingstg01.blob.core.windows.net",
  mount_point = "/mnt/iotdatamongedd",    # point that i can refer to which will get you to the container (soucre or path)(must be unique)
  extra_configs = {"fs.azure.account.key.otrainingstg01.blob.core.windows.net":dbutils.secrets.get(scope = "training01_finalProject", key = "destination-storage-access-key")})



# COMMAND ----------

# Calling necessary Libiraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import when
from pyspark.sql.functions import split, expr
from pyspark.sql.functions import split, concat, lpad, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ### PG Part Properties Report Table

# COMMAND ----------

  df = spark.read.csv("/mnt/iotdatamongedd/Training /Properties/Monged/PG Part Properties Report",header='true')

# COMMAND ----------

display(df)

# COMMAND ----------

df_after_drop = df.select(" Part", " Site", " Description", " EAN", " Brand", " Sub-Sector", " Segment","Product Form")


# COMMAND ----------

display(df_after_drop)

# COMMAND ----------

# Rname Colmns 
df_after_drop = df_after_drop.withColumnRenamed(" Part", "FPC").withColumnRenamed(" Site", "Site").withColumnRenamed(" Description", "Description").withColumnRenamed(" EAN", "EAN").withColumnRenamed(" Brand", "Brand").withColumnRenamed(" Sub-Sector", "Sub-Sector").withColumnRenamed(" Segment", "Segment")


# COMMAND ----------

display(df_after_drop)

# COMMAND ----------

# Create FPC Site column
df_after_drop = df_after_drop.withColumn("FPC_Site", concat(df_after_drop["FPC"], lit("_"), df_after_drop["Site"]))


# COMMAND ----------

display(df_after_drop)

# COMMAND ----------

# Convert PySpark DataFrame to Pandas DataFrame
pandas_df = df_after_drop.toPandas()

# Split the column based on the underscore character
split_col = pandas_df['FPC_Site'].str.split('_', expand=True)

# Extract FPC and Site
fpc = split_col[0]
site = split_col[1]

# Add leading zeros if Site is less than 4 digits
pandas_df['Site'] = site.str.zfill(4)

# Concatenate FPC and padded Site
pandas_df['FPC_Site'] = fpc + '_' + pandas_df['Site']

# Convert Pandas DataFrame back to PySpark DataFrame
df_after_drop = spark.createDataFrame(pandas_df)

# Show the result
display(df_after_drop)

# COMMAND ----------

# Rename columns to be saved as tables
new_columns = []
for col_name in df_after_drop.columns:
    new_col_name = col_name.replace(" ", "_").replace("-", "_")
    new_columns.append(f"`{col_name}` AS `{new_col_name}`")

# Apply the new column names to the DataFrame
df_after_drop = df_after_drop.selectExpr(*new_columns)

# COMMAND ----------

display(df_after_drop)

# COMMAND ----------

# Save the table in databricks 
df_after_drop.write.mode("overwrite").saveAsTable("dd_training.monged_PG_Matrial_Properties")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Order details Table

# COMMAND ----------

  orders_df = spark.read.csv("/mnt/iotdatamongedd/Training /OrderDetails/Monged/order details",header='true')

# COMMAND ----------

display(orders_df)

# COMMAND ----------

orders_df_after_drop = orders_df.select("Customer",	"Ship_To",	"Ship_To_Country",	"Number_Line",	"Order",	"Site",	"Target_MAD",	"current_MAD",	"Part",	"Quantity",	"On_Time",	"Late_Qty"	,"creation_date","RDD",	"GI_Date"
)

# COMMAND ----------

orders_df_after_drop = orders_df_after_drop.withColumnRenamed("Part", "FPC")



# COMMAND ----------

orders_df_after_drop = orders_df_after_drop.withColumn("FPC_Site", concat(orders_df_after_drop["FPC"], lit("_"), orders_df_after_drop["Site"]))
display(orders_df_after_drop)


# COMMAND ----------

# Rename columns to be saved as tables
new_columns = []
for col_name in orders_df_after_drop.columns:
    new_col_name = col_name.replace(" ", "_").replace("-", "_")
    new_columns.append(f"`{col_name}` AS `{new_col_name}`")

# Apply the new column names to the DataFrame
orders_df_after_drop = orders_df_after_drop.selectExpr(*new_columns)


# COMMAND ----------

display(orders_df_after_drop)

# COMMAND ----------

# Convert PySpark DataFrame to Pandas DataFrame
pandas_df = orders_df_after_drop.toPandas()

# Split the column based on the underscore character
split_col = pandas_df['FPC_Site'].str.split('_', expand=True)

# Extract FPC and Site
fpc = split_col[0]
site = split_col[1]

# Add leading zeros if Site is less than 4 digits
pandas_df['Site'] = site.str.zfill(4)

# Concatenate FPC and padded Site
pandas_df['FPC_Site'] = fpc + '_' + pandas_df['Site']

# Convert Pandas DataFrame back to PySpark DataFrame
orders_df_after_drop = spark.createDataFrame(pandas_df)

# Show the result
display(orders_df_after_drop)



# COMMAND ----------

# Save the table in databricks 
orders_df_after_drop.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dd_training.monged_order_details")

# COMMAND ----------

# MAGIC %md
# MAGIC ### On Hand Table

# COMMAND ----------

 onhand_df = spark.read.csv("/mnt/iotdatamongedd/Training /On Hand/Monged/on hand",header='true')

# COMMAND ----------

# convert the data to its original format
csv_string = onhand_df.toPandas().to_csv(index=False)
rows = csv_string.split('\n')

# COMMAND ----------

# Show data that contains more that 9 Commas
from pyspark.sql.functions import size


# Create DataFrame from the list
df = spark.createDataFrame([(value,) for value in transformed_data], ["value"])

# Split each value by commas and count the number of elements
split_count_df = df.selectExpr("value", "size(split(value, ',')) AS count")

# Filter out the rows where the count is not equal to 9
result_df = split_count_df.filter(split_count_df["count"] != 9)

# Show the filtered DataFrame
result_df.show(truncate=False)



# COMMAND ----------

# Create a DataFrame with a single string column
df = spark.createDataFrame([(row,) for row in transformed_data], ["csv_row"])

# Read the DataFrame as CSV with header
result_df = spark.read.csv(df.rdd.map(lambda x: x.csv_row), header=True)

# Show the DataFrame
display(result_df)

# COMMAND ----------

# Check handeling the error
result_df.count()
display(result_df[result_df['Part']==81725151])

# COMMAND ----------

# Select wanted columns
onhand_df_after_drop = result_df.select("Site",	"Part",	"Description",	"Quantity",	"Type")
# Choose Unrestricted on Type column
onhand_df_after_drop = onhand_df_after_drop[onhand_df_after_drop['Type'] == "Unrestricted"]

# COMMAND ----------

display(onhand_df_after_drop)

# COMMAND ----------

# Change column name and create FPC_Site column
onhand_df_after_drop = onhand_df_after_drop.withColumnRenamed("Part", "FPC")
onhand_df_after_drop = onhand_df_after_drop.withColumn("FPC_Site", concat(onhand_df_after_drop["FPC"], lit("_"), onhand_df_after_drop["Site"]))
display(onhand_df_after_drop)

# COMMAND ----------

# Convert PySpark DataFrame to Pandas DataFrame
pandas_df = onhand_df_after_drop.toPandas()

# Split the column based on the underscore character
split_col = pandas_df['FPC_Site'].str.split('_', expand=True)

# Extract FPC and Site
fpc = split_col[0]
site = split_col[1]

# Add leading zeros if Site is less than 4 digits
pandas_df['Site'] = site.str.zfill(4)

# Concatenate FPC and padded Site
pandas_df['FPC_Site'] = fpc + '_' + pandas_df['Site']

# Convert Pandas DataFrame back to PySpark DataFrame
onhand_df_after_drop = spark.createDataFrame(pandas_df)

# Show the result
display(onhand_df_after_drop)

# COMMAND ----------

# Rename columns to be saved as tables
new_columns = []
for col_name in onhand_df_after_drop.columns:
    new_col_name = col_name.replace(" ", "_").replace("-", "_")
    new_columns.append(f"`{col_name}` AS `{new_col_name}`")

# Apply the new column names to the DataFrame
onhand_df_after_drop = onhand_df_after_drop.selectExpr(*new_columns)


# COMMAND ----------


display(onhand_df_after_drop)

# COMMAND ----------

onhand_df_after_drop.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, sum

onhand_df_after_drop = onhand_df_after_drop.withColumn("Quantity", col("Quantity").cast("Float"))

onhand_df_after_drop = onhand_df_after_drop.groupBy('Site', 'FPC', 'Description', 'Type','FPC_Site').agg(sum(col("Quantity")).alias("Quantity"))

# COMMAND ----------

display(onhand_df_after_drop[onhand_df_after_drop['FPC_Site'] == "80202362_8347" ])

# COMMAND ----------

onhand_df_after_drop = onhand_df_after_drop.withColumn("Quantity", col("Quantity").cast("string"))
onhand_df_after_drop.printSchema()

# COMMAND ----------

# Save data in databricks
onhand_df_after_drop.write.mode("overwrite").saveAsTable("dd_training.monged_onhand")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import IOPT Extract Table

# COMMAND ----------

 iopt_df = spark.read.csv("/mnt/iotdatamongedd/Training /IOPT/Monged/Import IOPT Extract",header='true')

# COMMAND ----------

iopt_df_after_drop = iopt_df.select("SMO",	"LCS Code"	,"LCS Name",	"PI FPC",  "Production Plants", "PO FPC Description","PI FPC  Description",	"PO FPC",	"Customer Conversion",	"Current Start Of Sale"	,"End Of Sale (of PO or discontinuation)",	"Product Brand"
)

# COMMAND ----------

from pyspark.sql.functions import split, explode


# Split the values in "col3" by comma and explode it into multiple rows
iopt_df_after_drop = iopt_df_after_drop.toPandas()
iopt_df_after_drop['Production Plants'] = iopt_df_after_drop['Production Plants'].str.split(',')
# Convert list into multiple rows
iopt_df_after_drop = iopt_df_after_drop.explode('Production Plants')
iopt_df_after_drop = spark.createDataFrame(iopt_df_after_drop)

# COMMAND ----------

from pyspark.sql.functions import col

filtered_df = iopt_df_after_drop.filter((col("PI FPC").isNotNull()))

# Select columns "PO FPC" and "LCS Name"
phase_out_df = filtered_df.select("PI FPC","PO FPC", "LCS Name", "LCS Code"	,"Production Plants", "PO FPC Description", "Customer Conversion",	"Current Start Of Sale"	,"End Of Sale (of PO or discontinuation)")

# Remove duplicate rows
#phase_out_df = selected_df.dropDuplicates(subset=["PI FPC", "PO FPC", "LCS Name"])

# Show the result
phase_out_df.show()

# COMMAND ----------


# Rename column "PO FPC" to "pipo"
phase_out_df = phase_out_df.withColumnRenamed("PO FPC", "PIPO_FPC")
phase_out_df = phase_out_df.withColumnRenamed("PI FPC", "FPC")

# Add column "pipo_type" with constant value "Phase Out"
phase_out_df = phase_out_df.withColumn("PIPO_Type", lit("Phase Out"))


# COMMAND ----------

# Create or extract the phase in Data
# Select columns "PO FPC" and "LCS Name"
filtered_df2 = iopt_df_after_drop.filter((col("PO FPC").isNotNull()))
phase_in_df = filtered_df2.select("PO FPC","PI FPC", "LCS Name", "LCS Code", "Production Plants","PO FPC Description", "Customer Conversion",	"Current Start Of Sale"	,"End Of Sale (of PO or discontinuation)")

# Remove duplicate rows
#phase_in_df = selected_df2.dropDuplicates(subset=["PI FPC", "PO FPC", "LCS Name"])

# Show the result
phase_in_df.show()

# COMMAND ----------


# Rename column "PO FPC" to "pipo"
phase_in_df = phase_in_df.withColumnRenamed("PI FPC", "PIPO_FPC")
phase_in_df = phase_in_df.withColumnRenamed("PO FPC", "FPC")


# Add column "pipo_type" with constant value "Phase Out"
phase_in_df = phase_in_df.withColumn("PIPO_Type", lit("Phase In"))

# COMMAND ----------

pipo_df = phase_out_df.union(phase_in_df)
pipo_df = pipo_df.withColumn("FPC_Site", concat(col("FPC"), lit("_"), col("Production Plants")))

display(pipo_df)

# COMMAND ----------

# Rename columns to be saved as tables
new_columns = []
for col_name in pipo_df.columns:
    new_col_name = col_name.replace(" ", "_").replace("-", "_").replace("(","").replace(")","")
    new_columns.append(f"`{col_name}` AS `{new_col_name}`")

# Apply the new column names to the DataFrame
pipo_df = pipo_df.selectExpr(*new_columns)
display(pipo_df)

# COMMAND ----------

pipo_df = pipo_df.withColumn("FPC_Type", when(pipo_df["PIPO_Type"] == "Phase In", "Phase Out").otherwise("Phase In"))


# COMMAND ----------

pipo_df.count()

# COMMAND ----------

# Convert PySpark DataFrame to Pandas DataFrame
pandas_df = pipo_df.toPandas()

# Split the column based on the underscore character
split_col = pandas_df['FPC_Site'].str.split('_', expand=True)

# Extract FPC and Site
fpc = split_col[0]
site = split_col[1]

# Add leading zeros if Site is less than 4 digits
pandas_df['Site'] = site.str.zfill(4)

# Concatenate FPC and padded Site
pandas_df['FPC_Site'] = fpc + '_' + pandas_df['Site']

# Convert Pandas DataFrame back to PySpark DataFrame
pipo_df = spark.createDataFrame(pandas_df)

# Show the result
display(pipo_df)

# COMMAND ----------

# Save data in databricks
pipo_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dd_training.monged_pipo")

# COMMAND ----------


