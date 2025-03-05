

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import pandas_udf, col, when, lit


#  Count number of optional services enabled, like streaming TV
def compute_service_features(inputDF: SparkDataFrame) -> SparkDataFrame:
  # Create pandas UDF function
  @pandas_udf('double')
  def num_optional_services(*cols):
    # Nested helper function to count number of optional services in a pandas dataframe
    return sum(map(lambda s: (s == "Yes").astype('double'), cols))

  return inputDF.\
    withColumn("num_optional_services",
        num_optional_services("online_security", "online_backup", "device_protection", "tech_support", "streaming_tv", "streaming_movies"))

### Using Pandas On Spark API

# Because our Data Scientist team is familiar with Pandas, we'll use the [pandas on spark API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/index.html) to scale `pandas` code. The Pandas instructions will be converted in the spark engine under the hood and distributed at scale.

# *Note: Starting from `spark 3.2`, koalas is builtin and we can get an Pandas Dataframe using `pandas_api()`.*

def clean_churn_features(dataDF: SparkDataFrame) -> SparkDataFrame:
  """
  Simple cleaning function leveraging pandas API
  """

  # Convert to pandas on spark dataframe
  data_psdf = dataDF.pandas_api()

  # Convert some columns
  data_psdf = data_psdf.astype({"senior_citizen": "string"})
  data_psdf["senior_citizen"] = data_psdf["senior_citizen"].map({"1" : "Yes", "0" : "No"})

  data_psdf["total_charges"] = data_psdf["total_charges"].apply(lambda x: float(x) if x.strip() else 0)

  # Fill some missing numerical values with 0
  data_psdf = data_psdf.fillna({"tenure": 0.0})
  data_psdf = data_psdf.fillna({"monthly_charges": 0.0})
  data_psdf = data_psdf.fillna({"total_charges": 0.0})

  # Add/Force semantic data types for specific colums (to facilitate autoML)
  data_cleanDF = data_psdf.to_spark()
  data_cleanDF = data_cleanDF.withMetadata("customer_id", {"spark.contentAnnotation.semanticType":"native"})
  data_cleanDF = data_cleanDF.withMetadata("num_optional_services", {"spark.contentAnnotation.semanticType":"numeric"})

  return data_cleanDF