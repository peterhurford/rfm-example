Making an RFM (Recency, Frequency, Monetization) model from the [Acquire Value Shoppers Challenge](https://www.kaggle.com/c/acquire-valued-shoppers-challenge).


### Installation

1.) Install Python, git, etc.

2.) Clone this repo.

3.) Within the repo, create a `data` folder and unzip the [Acquire Value Shoppers Challenge](https://www.kaggle.com/c/acquire-valued-shoppers-challenge) data (`transactions.csv`) into that folder.

4.) Create 10M subset data to work on: `tail -n 10000000 data/transactions.csv > trans10m.csv`

5.) `pip install python-dateutil`

6.) Install [Vowpal Platypus](https://github.com/peterhurford/vowpal_platypus)


### Run Example

```
python rfm.py --cores 4
```


### Bakeoff Against Spark

7.) Install [Pyspark](http://spark.apache.org/docs/latest/api/python/pyspark.html)

Run one at a time and benchmark:

```
python rfm.py --cores 4 # Python example
spark-submit --master local[4] --driver-memory 8G --packages com.databricks:spark-csv_2.11:1.5.0 rfm_spark.py --partitions 16 # Spark example
```

**Benchmarks on my laptop** (Macbook 15-inch Mid-2015 16 GB RAM using OS X 10.11.6)
* Python v2.7.12: 10M in 2m21s (0.0141ms/row) with 4 cores
* Pyspark v1.6.2: 10M in 6m15s (0.0375ms/row) with 4 cores

**Benchmarks on AWS EC2 m4.10xlarge** (40 core, 160GB RAM)
* Python v2.7.12: 350M (full set) in 50m9s (0.008606ms/row) with 40 cores (costs $2.16)
