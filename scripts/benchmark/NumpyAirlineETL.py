import pandas as pd
import time
import warnings
warnings.filterwarnings('ignore')
print("LoadDataSet")
source = pd.read_csv("/combined.csv", delimiter="|")

def udf(flights):
    c1 = flights[(flights.Cancelled.eq(False)) & (flights.DepDelay.ge(10)) & (
            (flights.IATA_CODE_Reporting_Airline.eq("AA")) | (flights.IATA_CODE_Reporting_Airline.eq("HA")))]
    avgDelay = ((c1["DepDelay"].add(c1["ArrDelay"])).div(2))
    c1["avgDelay"] = avgDelay
    c1.loc[c1.avgDelay > 30, 'delay'] = "High"
    c1.loc[c1.avgDelay < 30, 'delay'] = "Medium"
    c1.loc[c1.avgDelay < 20, 'delay'] = "Low"

    res = c1[["DepDelay", "delay"]]
    return res



for n in range(1, 100):
    print("Run benchmark in iteration: " + str(n))
    start = time.time()
    udf(source)

    end = time.time()
    print(end - start)
    # print(res)


