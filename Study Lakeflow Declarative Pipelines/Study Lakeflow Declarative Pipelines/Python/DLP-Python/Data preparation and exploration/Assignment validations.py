# Databricks notebook source
# DBTITLE 1,Cell 1
import sys
import os

# Add the parent directory to Python path to enable imports from utilities
parent_dir = os.path.dirname(os.path.abspath(os.getcwd()))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from utilities.validation_utilities import validate_streaming_table,validate_mv,validate_delta

# COMMAND ----------

dbutils.widgets.text("unity_catalog","learn_adb_fikrat")
uc_name=dbutils.widgets.get("unity_catalog")
spark.sql(f'USE CATALOG {uc_name}');

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets/retail-org/"))

# COMMAND ----------

display(spark.read.format("json").load("dbfs:/databricks-datasets/retail-org/sales_orders/"))

# COMMAND ----------

display(spark.read.csv("dbfs:/databricks-datasets/retail-org/customers/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Tables

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1.** Add a streaming table for sales orders. 
# MAGIC - The streaming table will source data from this path: dbfs:/databricks-datasets/retail-org/sales_orders/ 
# MAGIC - Name the target table as 'sales' and place it in pipeline's default catalog's _Bronze_ schema

# COMMAND ----------

# DBTITLE 1,Step 1
validate_streaming_table('bronze','sales')

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2.** Create a streaming table for customers. 
# MAGIC - The streaming table will source data from this path: dbfs:/databricks-datasets/retail-org/customers/ 
# MAGIC - Name the target table as 'customers' and place it in pipeline's default catalog's _Bronze_ schema

# COMMAND ----------

# DBTITLE 1,Step 2
validate_streaming_table('bronze','customers')

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3**. Create a streaming table named 'sales_orders' in the Silver schema, sourced from _bronze.sales_ table. **Instructions**: 
# MAGIC - The table sales_orders should include only the following columns: order_number,order_datetime,customer_id, 
# MAGIC  customer_name,number_of_line_items

# COMMAND ----------

# DBTITLE 1,Step 3
validate_streaming_table('silver','sales_orders')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Controls

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 4.** Create a streaming table named 'customers' in Silver schema, sourced from _bronze.customers_ table. Instructions: 
# MAGIC - Use Data Quality validation expectations to filter out  customers having empty city attributes.
# MAGIC - Use Data Quality validation expectations of warning type for customers having empty region attributes.
# MAGIC - The target table should include only the following columns: customer_id, customer_name, city, state,region

# COMMAND ----------

# DBTITLE 1,Step 4
validate_streaming_table('silver','customers')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialized views

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 5**. Create a materialized view named 'mv_sales_order_items' in the Silver schema, sourced from _Bronze.sales_ table.The definion of materilazied view should include following transformations:
# MAGIC - Add Identity column named item_id, using _monotonically_increasing_id_() function. 
# MAGIC - Parse ordered_products column to separate each ordered item, using _explode_ function.
# MAGIC - Extract sub-fields under ordered_products
# MAGIC - Calculate amount field by multiplying _qty_ to _price_ column
# MAGIC - The table should include only the following columns: "item_id","order_number","product_id", "product_name", "currency","unit","price", "qty","amount"
# MAGIC

# COMMAND ----------

# DBTITLE 1,Step 5
validate_mv('silver','mv_sales_order_items')

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 6.** Create a materialized view named 'mv_sales_order_aggregates' in the Silver schema. **Instructions**:
# MAGIC - Join silver.sales_orders and silver.mv_sales_order_items tables on order_number column.
# MAGIC - Group by order_number, calculate the sum of amount column, and name it total_amount
# MAGIC - The target table should be a Delta Lake table and should include only the following columns: "order_number","total_amount"

# COMMAND ----------

# DBTITLE 1,Step 6.
validate_mv('silver','mv_sales_order_aggregates')

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 7**.Create a materialized view named 'mv_sales_orders_customers' in the Silver schema. **Instructions**:
# MAGIC - Join _silver.mv_sales_order_aggregates, silver.sales_orders and silver.customers_ tables on order_number column.
# MAGIC - Group by _order_number_, calculate the sum of _amount_ column, and name it _total_amount_
# MAGIC - The target table should be a Delta Lake table and should include only the following columns: "order_number","order_datetime","number_of_line_items","total_amount","customer_id","customer_name","city","state"

# COMMAND ----------

# DBTITLE 1,Step 7
validate_mv('silver','mv_sales_orders_customers')

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 8**. TO DO: Create a materialized view named '_mv_sales_orders_aggregates_by_state_' in the Silver schema. **Instructions**:
# MAGIC - Group by "state","city" columns and calculate the sum of total_amount column, and name it as _total_amount_$_
# MAGIC - The target table should be a Delta Lake table and should include only the following columns: "state","city","total_amount_$"
# MAGIC - Order by "state","city" columns

# COMMAND ----------

# DBTITLE 1,Step 8
validate_mv('silver','mv_sales_orders_aggregates_by_state')

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 9**. Create a Delta Lake sink named '_sales_orders_aggregates_delta_lake_' in the _Silver_ schema. The target table should be a Delta Lake table and should include only the following columns: "state","city","total_amount_$"
# MAGIC - Tip: Use catalog name paramater to specify the catalog name for the target table

# COMMAND ----------

# DBTITLE 1,Step 9
validate_delta('silver','sales_orders_aggregates_delta_lake')

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select table_type from INFORMATION_SCHEMA.TABLES
# MAGIC                  where table_name= 'sales_orders_aggregates_delta_lake' and table_schema='silver'

# COMMAND ----------

df=spark.read.load("/databricks-datasets/tpch/delta-001/orders")
display(df)

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/tpch/delta-001/"))

# COMMAND ----------

df=spark.read.load("/databricks-datasets/tpch/delta-001/lineitem")
display(df)

# COMMAND ----------

