# Example usage: spark-submit --master local[4] --driver-memory 8G --packages com.databricks:spark-csv_2.11:1.5.0 rfm_spark.py --partitions 16

import argparse
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta  # pip install python-dateutil
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, udf, lit, asc, explode
from pyspark.sql.functions import max as fmax, sum as fsum, mean as fmean
from pyspark.sql.types import DateType, IntegerType, DoubleType
from pyspark.ml.feature import MinMaxScaler, VectorAssembler

start = datetime.now()
parser = argparse.ArgumentParser()
parser.add_argument('--partitions')
partitions = int(parser.parse_args().partitions)

sc = SparkContext(appName='PySparkALSTest')
sc.setLogLevel('ERROR')
sql_context = SQLContext(sc)

to_date = udf(lambda x: datetime.strptime(x, '%Y-%m-%d'), DateType())

transactions = (sql_context.read
                .format('com.databricks.spark.csv')
                .option('header', 'true')
                .option('inferSchema', 'true')
                .load('file://' + os.getcwd() + '/data/trans10m.csv')
                .repartition(partitions)
                .withColumn('date2', to_date(col('date')))
                .select('id',
                        'date2',
                        col('purchasequantity').alias('quantity'),
                        col('purchaseamount').alias('amount'))
                .select('id', col('date2').alias('date'), 'quantity', 'amount'))

def subtract_ending_date(date):
    ending_date = datetime(2013, 7, 27, 0, 0)
    return (ending_date.date() - date).days
udf_subtract_ending_date = udf(subtract_ending_date, IntegerType())
    
ending_date = datetime(2013, 7, 27, 0, 0)
one_year_ago = ending_date - relativedelta(years=1)
rfm = (transactions.where(col('date') >= one_year_ago)
                   .groupBy('id')
                   .agg(fmax('date'),
                        fsum('quantity').alias('frequency'),
                        fmean('amount').alias('monetization'))
                   .withColumn('recency', udf_subtract_ending_date(col('max(date)')))
                   .select('id', 'recency', 'frequency', 'monetization'))
vect_rfm = VectorAssembler(inputCols=['recency'], outputCol='recency_v').transform(rfm)
vect_rfm = VectorAssembler(inputCols=['frequency'], outputCol='frequency_v').transform(vect_rfm)
vect_rfm = VectorAssembler(inputCols=['monetization'], outputCol='monetization_v').transform(vect_rfm)
scaled_rfm = MinMaxScaler(inputCol='recency_v', outputCol='recency_s').fit(vect_rfm).transform(vect_rfm)
scaled_rfm = MinMaxScaler(inputCol='frequency_v', outputCol='frequency_s').fit(scaled_rfm).transform(scaled_rfm)
scaled_rfm = MinMaxScaler(inputCol='monetization_v', outputCol='monetization_s').fit(scaled_rfm).transform(scaled_rfm)

udf_unvector = udf(lambda x: float(x[0]), DoubleType())
scored_rfm = (scaled_rfm.withColumn('recency_ss', udf_unvector('recency_s'))
              .withColumn('frequency_ss', udf_unvector('frequency_s'))
              .withColumn('monetization_ss', udf_unvector('monetization_s'))
              .withColumn('rfm_score', col('recency_ss') + col('frequency_ss') + col('monetization_ss'))
              .select('id', 'recency', 'frequency', 'monetization', 'rfm_score')
              .orderBy(asc('rfm_score')))
scored_rfm.show()
