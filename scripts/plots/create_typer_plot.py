import pandas as pd


def getQueryString(string):
    return string.split("_")[0]


data = pd.read_csv("../typer.out", index_col=False, names=["query", "hand_ts"])

data["query"] = data["query"].apply(lambda x: x.strip())

data[data["query"] == "q1 hyper"]["query"] = "q1"
data[data["query"] == "q3 hyper"]["query"] = "q3"
data[data["query"] == "q6 hyper"]["query"] = "q6"
data[data["query"] == "q18 hyper"]["query"] = "q18"
data[data["query"] == "q1.1 hyper"]["query"] = "ssb11"
data[data["query"] == "q2.1 hyper"]["query"] = "ssb21"
data[data["query"] == "q3.1 hyper"]["query"] = "ssb31"
data[data["query"] == "q4.1 hyper"]["query"] = "ssb41"
data.to_csv('relational_plot_data_typer.csv')

