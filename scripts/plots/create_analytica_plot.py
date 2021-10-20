import pandas as pd


def getQueryString(string):
    return string.split("_")[0]


data = pd.read_csv("../../results/AnalyticsBenchmark.csv", index_col=False, names=["data", "query", "language", "lazy",
                                                                                   "ts_1_min", "ts_1_max", "ts_1_avg",
                                                                                   "ts_2_min", "ts_2_max", "ts_2_avg"])

js = data[data["language"] == "js"]
js = js[["ts_2_avg","query"]].reset_index()
js = js.rename(columns={"ts_2_avg": "js_ts"})

java = data[data["language"] == "java"]
java = java[["ts_2_avg"]].reset_index()
java = java.rename(columns={"ts_2_avg": "java_ts"})

python = data[data["language"] == "python"]
python = python[["ts_2_avg"]].reset_index()
python = python.rename(columns={"ts_2_avg": "python_ts"})

typer = pd.read_csv("../hand.out", names=["query", "hand_ts"])

result = pd.concat([js, java, python, typer], axis=1, sort=False)

result = result[["query","python_ts", "js_ts", "java_ts", "hand_ts"]]

result.to_csv('analytica_plot_data_js.csv')
