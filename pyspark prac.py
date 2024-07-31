# Databricks notebook source
import pandas as pd
import numpy as np
from pyspark.sql.functions import *
from pyspark.sql.functions import current_date

# COMMAND ----------

# Setting configuration parameters to read the SHS refreshed date - shsenddate
shsenddate = sqlContext.sql("Select string(max(SERVICE_DATE)) as shsenddate from immuno_stage.laad_acthar_fact_rx").collect()[0]["shsenddate"]
spark.conf.set('shsenddate.shs_end_date',shsenddate)

# COMMAND ----------

# MAGIC %sql
# MAGIC --select count (1) from patview
# MAGIC select * from immuno_stage.laad_acthar_fact_diagnosis limit 1

# COMMAND ----------

from pyspark.sql.types import StringType,BooleanType,DateType
#for RX
df_fact_rx = spark.table("immuno_stage.laad_acthar_fact_rx")
df_mkt_def = spark.table("IMMUNO_MASTER.OVERALL_MKT_DEF_FINAL")
df_excl = spark.sql("select distinct activity_code as ACTIVITY_TYPE from IMMUNO_MASTER.OVERALL_MKT_DEF_FINAL_EXCLUSION where activity='RX'")
df_inter = df_mkt_def.join(df_mkt_def.select('ACTIVITY_TYPE').subtract(df_excl.select('ACTIVITY_TYPE')), on="ACTIVITY_TYPE").filter(col("Activity_Type") == 'RX').select("NDC")
df_mkt_def_rx = df_fact_rx.join(df_inter, on ="NDC").select(df_fact_rx.patient_id.alias("PATIENT_ID"))
#for PX
df_fact_px = spark.table("immuno_stage.laad_acthar_fact_procedure")
df_inter = df_mkt_def.filter(col("Activity_Type") == 'PX').select("Activity_Code")
df_mkt_def_px = df_fact_px.join(df_inter,df_fact_px.PX_CODE == df_inter.Activity_Code, "inner" ).select(df_fact_px.PATIENT_ID)

#for DX
df_fact_dx = spark.table("immuno_stage.laad_acthar_fact_diagnosis")
df_inter = df_mkt_def.filter(col("Activity_Type") == 'DX').select("Activity_Code")
df_mkt_def_dx = df_fact_dx.join(df_inter,df_fact_dx.DX_CODE == df_inter.Activity_Code, "inner" ).select(df_fact_dx.PATIENT_ID)
#display(df_mkt_def_px)

df_total_pat = df_mkt_def_rx.unionAll(df_mkt_def_px).unionAll(df_mkt_def_dx).distinct()
display(df_total_pat)
#df_total_pat.createOrReplaceTempView('tot_pat')

# COMMAND ----------

df_total_pat.createOrReplaceTempView('tot_pat')
