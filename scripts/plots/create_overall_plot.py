import pandas as pd


def getQueryString(string):
    return string.split("_")[0]


data = pd.read_csv("relational_plot_data_js.csv", index_col=0)

data2 = pd.read_csv("analytica_plot_data_js.csv", index_col=0)

result = data.append(data2).reset_index()

result.to_csv('overall_plot_data_js.csv')
