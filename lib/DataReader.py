# Importing the necessary module
from lib import ConfigReader

# Defining customers schema
def get_customers_schema():
    schema = """
        customer_id int,
        customer_fname string,
        customer_lname string,
        username string,
        password string,
        address string,
        city string,
        state string,
        pincode string
    """
    return schema

# Creating customers DataFrame
def read_customers(spark, env):
    # Get configuration based on environment
    conf = ConfigReader.get_app_config(env)
    
    # Get the file path from the configuration
    customers_file_path = conf["customers.file.path"]
    
    # Read the CSV file into a DataFrame
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(get_customers_schema()) \
        .load(customers_file_path)

# Defining orders schema
def get_orders_schema():
    schema = """
        order_id int,
        order_date string,
        customer_id int,
        order_status string
    """
    return schema

# Creating orders DataFrame
def read_orders(spark, env):
    # Get configuration based on environment
    conf = ConfigReader.get_app_config(env)
    
    # Get the file path from the configuration
    orders_file_path = conf["orders.file.path"]

    # Read the CSV file into a DataFrame
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(get_orders_schema()) \
        .load(orders_file_path)
