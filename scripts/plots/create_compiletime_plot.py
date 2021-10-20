import pandas as pd
import math
import numpy as np

def getQueryString(string):
    return string.split("_")[0]


data = pd.read_csv("../../results/CompileTimeBenchmark.csv", index_col=False, names=["data", "query", "language", "lazy",
                                                                                   "ts_1",
                                                                                   "ts_2"])

data["tp"] = (1000/data["ts_1"])*6001214
js = data[data["language"] == "js"]
js["ts"] = js["ts_1"].cumsum()
js["ts_sec"] = (js["ts"] / 1000).apply(np.floor)
mean_js = js.groupby(['ts_sec', 'language']).mean()
mean_js = mean_js[["tp"]].reset_index()
mean_js["ts_sec"] = mean_js["ts_sec"]
mean_js = mean_js.set_index("ts_sec")
mean_js.to_csv('compile_time_js.csv')

data["tp"] = (1000/data["ts_1"])*6001214
js = data[data["language"] == "python"]
js["ts"] = js["ts_1"].cumsum()
js["ts_sec"] = (js["ts"] / 1000).apply(np.floor)
mean_js = js.groupby(['ts_sec', 'language']).mean()
mean_js = mean_js[["tp"]].reset_index()
mean_js["ts_sec"] = mean_js["ts_sec"]
mean_js = mean_js.set_index("ts_sec")
mean_js.to_csv('compile_time_python.csv')

data["tp"] = (1000/data["ts_1"])*6001214
js = data[data["language"] == "rel"]
js["ts"] = js["ts_1"].cumsum()
js["ts_sec"] = (js["ts"] / 1000).apply(np.floor)
mean_js = js.groupby(['ts_sec', 'language']).mean()
mean_js = mean_js[["tp"]].reset_index()
mean_js["ts_sec"] = mean_js["ts_sec"]
mean_js = mean_js.set_index("ts_sec")
mean_js.to_csv('compile_time_rel.csv')

data["tp"] = (1000/data["ts_1"])*6001214
js = data[data["language"] == "java"]
js["ts"] = js["ts_1"].cumsum()
js["ts_sec"] = (js["ts"] / 1000).apply(np.floor)
mean_js = js.groupby(['ts_sec', 'language']).mean()
mean_js = mean_js[["tp"]].reset_index()
mean_js["ts_sec"] = mean_js["ts_sec"]
mean_js = mean_js.set_index("ts_sec")
mean_js.to_csv('compile_time_java.csv')


#java = data[data["language"] == "java"]
#java = java[["ts_2_avg"]].reset_index()
#java = java.rename(columns={"ts_2_avg": "java_ts"})

#python = data[data["language"] == "python"]
#python = python[["ts_2_avg"]].reset_index()
#python = python.rename(columns={"ts_2_avg": "python_ts"})

#typer = pd.read_csv("../hand.out", names=["query", "hand_ts"])

#result = pd.concat([js, java, python, typer], axis=1, sort=False)

#result = result[["query","python_ts", "js_ts", "java_ts", "hand_ts"]]

#result.to_csv('analytica_plot_data_js.csv')
