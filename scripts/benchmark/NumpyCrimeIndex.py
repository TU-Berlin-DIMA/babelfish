import warnings

import numpy as np
import pandas as pd
import time
warnings.filterwarnings('ignore')
print("LoadDataSet")
source = pd.read_csv("~/projects/luth-org/us_cities_crime_data.csv", delimiter=",")


def udf(data):
    data_big_cities = data[data["Total population"] > 500000]
    # Compute "crime index" proportional to
    # exp((Total population + 2*(Total adult population) - 2000*(Number of
    # robberies)) / 100000)
    data_big_cities["Crime Index"] = data_big_cities["Total population"] * 1.0 + (
            data_big_cities["Total adult population"] * 2.0) + (data_big_cities["Number of robberies"] * -2000.0)
    data_big_cities["Crime index"] = np.exp(data_big_cities["Crime Index"] / 100000.0)
    #data_big_cities["Crime index"][data_big_cities["Crime index"] >= 0.02] = 0.02
    #data_big_cities["Crime index"][data_big_cities["Crime index"] < 0.01] = 0.01
    return data_big_cities["Crime index"]


# predictions = predictions / predictions.sum()
# data_big_cities_new_df["Crime index"] = predictions

# Aggregate "crime index" scores by state
# data_big_cities_grouped_df = data_big_cities_new_df.groupby(
#    "State short").sum()
# return data_big_cities_grouped_df


for n in range(1, 100):
    print("Run benchmark in iteration: " + str(n))
    start = time.time()
    udf(source)
    end = time.time()
    print(end - start)
    # print(res)
