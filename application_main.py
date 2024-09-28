import spark as spark

from lib.logger import Log4j

print("Hello Python")

import sys
from lib import DataManipulation,DataReader,Utils
from pyspark.sql.functions import *

from pyspark.sql import SparkSession

if __name__ == '__main__':

    # if len(sys.argv) < 2:
    #     print("please specify the environment")
    #     sys.exit(-1)

    # job_run_env = sys.argv[1]
    job_run_env = "LOCAL"
    # print(job_run_env)

    print("!!!Creating Spark Session!!!")


    # Access the Log4j logger
    spark = SparkSession.builder.appName("Retail Analysis").getOrCreate()

    # Set the log level to INFO
    spark.sparkContext.setLogLevel("INFO")

    # Access the Log4j logger
    log4jLogger = spark._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)

  #  spark = Utils.get_spark_session(job_run_env)
  # logger = Log4j(spark)
  #logger.warn("Created Spark Session!!!")
  #print("Created Spark Session!!!")

    # Log a warning message
    logger.warn("Created Spark Session!!!")

    orders_df = DataReader.read_orders(spark,job_run_env)
    print("@@@orders_df_value@@@",orders_df)

    orders_filtered = DataManipulation.filter_closed_orders(orders_df)
    print("@@@orders_filtered_value@@@", orders_filtered)

    customer_df = DataReader.read_customers(spark,job_run_env)
    print("@@@customers_df_value@@@", customer_df)

    joined_df = DataManipulation.join_orders_customers(orders_filtered,customer_df)
    print("@@@joined_df_value@@@",joined_df)

    aggregated_results = DataManipulation.count_order_state(joined_df)
    print("@@@aggregated_results@@@",aggregated_results)

    aggregated_results.show()

    print("this line of code is for feature1 only ")
    logger.warn("this is the end of main")

<<<<<<< HEAD
### feature 2 branch new codecl
    #print("end of main")
=======

    print("end of main")
>>>>>>> 5f359f25c229143c3f44cd123d41e1a89a34e508


