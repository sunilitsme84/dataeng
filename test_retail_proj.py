import pytest
from lib.Utils import get_spark_session
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import filter_closed_orders,count_order_state,filter_orders_generic
from lib.ConfigReader import get_app_config

@pytest.mark.skip
def test_read_customers_df(spark):
    customers_count = read_customers(spark,"LOCAL").count()
    assert customers_count == 24

@pytest.mark.skip
def test_read_orders_df(spark):
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count == 24

# @pytest.mark.transformation()
# def test_filter_closed_orders(spark):
#     orders_df = read_orders(spark,"LOCAL")
#     filtered_count = filter_closed_orders(orders_df).count()
#     assert filtered_count == 7

@pytest.mark.skip("work in progress")
def test_read_app_config(spark):
    config = get_app_config("LOCAL")
    assert config ["orders.file.path"] == "data/orders_new.csv"

# @pytest.mark.transformation()
# def test_count_order_state(spark,expected_result):
#     customers_df = read_customers(spark,"LOCAL")
#     actual_results = count_order_state(customers_df)
#     print("actual_results", actual_results.collect)
#     assert actual_results.collect() == expected_result.collect()

@pytest.mark.parametrize(
    "status,count",
    [("CLOSED",7),
     ("COMPLETED", 6),
     ("PROCESSING", 6)
     ]
)
def test_status_count(spark,status,count):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_orders_generic(orders_df,status)
    assert filtered_count.count() == count




##command to Run######
#C:\Users\user\.virtualenvs\Retail_Analysis_New-bNk30t_t\Scripts\python.exe -m pytest

#C:\Users\user\.virtualenvs\Retail_Analysis_New-bNk30t_t\Scripts\python.exe -m pytest -v
## -v verbose