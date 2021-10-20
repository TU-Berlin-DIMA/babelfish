import pandas as pd

def getQueryString(string):
    return string.split("_")[0]

data = pd.read_csv("../../results/AnalyticsBenchmark.csv",index_col=False, names=["data", "query", "language", "lazy",
"ts_1_min", "ts_1_max", "ts_1_avg", "ts_2_min", "ts_2_max", "ts_2_avg"])
ds = data[data["language"]=="java"]

nr_records_in_orders = 1331900

ds["ts_1_min"] = (1000.0 / ds["ts_1_min"])  * nr_records_in_orders / 1000000
ds["ts_1_max"] = (1000.0 / ds["ts_1_max"])  * nr_records_in_orders / 1000000
ds["ts_1_avg"] = (1000.0 / ds["ts_1_avg"])  * nr_records_in_orders / 1000000
ds["ts_2_min"] = (1000.0 / ds["ts_2_min"])  * nr_records_in_orders / 1000000
ds["ts_2_max"] = (1000.0 / ds["ts_2_max"])  * nr_records_in_orders / 1000000
ds["ts_2_avg"] = (1000.0 / ds["ts_2_avg"])  * nr_records_in_orders / 1000000


not_naive = ds[ds["query"].str.contains("naive")==False]
not_naive = not_naive.set_index('query')

not_lazy = not_naive[not_naive["lazy"] == False]
not_lazy = not_lazy[["ts_1_avg", "ts_2_avg"]]
not_lazy = not_lazy.rename(columns={"ts_1_avg": "notlazyts1", "ts_2_avg": "notlazyts2"})



lazy = not_naive[not_naive["lazy"] == True]
lazy = lazy[["ts_1_avg", "ts_2_avg"]]
lazy = lazy.rename(columns={"ts_1_avg": "lazyts1", "ts_2_avg": "lazyts2"})


naive = ds[ds["query"].str.contains("naive")]
naive["query"] = naive["query"].apply(getQueryString)
naive = naive.set_index('query')
naive = naive[["ts_1_avg", "ts_2_avg"]]
naive = naive.rename(columns={"ts_1_avg": "naivets1", "ts_2_avg": "naivets2"})


result = pd.concat([not_lazy, lazy,naive], axis=1, sort=False)

result=result.reset_index()

#sorted = ds.sort_values(by=['query', 'lazy'], ascending=False)
#result = sorted[["query",'lazy', "ts2_avg"]]
result.to_csv('stringBenchmark_plot_data_java.csv')


