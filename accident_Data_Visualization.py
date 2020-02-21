import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

data = pd.read_csv("C:/Users/Excalibur/Documents/heatmap/year.csv")
heatmap_data = pd.pivot_table(data, values='count', index=['month'], columns='year')
heatmap_data.index = pd.CategoricalIndex(heatmap_data.index, categories=[
                                         "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"])
heatmap_data.sortlevel(level=0, inplace=True)
fig, ax = plt.subplots()
fig.set_size_inches(14, 10)
sns.heatmap(heatmap_data, cmap="YlGnBu")

acc_data = pd.read_csv("C:/Users/Excalibur/Documents/acc_analysis/acc_analysis.csv")
fig, ax = plt.subplots()
fig.set_size_inches(10, 10)
sns.barplot(x="count", y="Weather_Conditions", hue="Accident_Severity", data=acc_data)

cas_data = pd.read_csv("C:/Users/Excalibur/Documents/cas_analysis/cas_analysis.csv")
fig, ax = plt.subplots()
fig.set_size_inches(10, 10)
sns.barplot(x="count", y="Casualty_Type", hue="Accident_Severity", data=cas_data)

hour_data = pd.read_csv("C:/Users/Excalibur/Documents/hour_analysis/hour_analysis.csv")
fig, ax = plt.subplots()
fig.set_size_inches(15, 10)
sns.barplot(x="hour", y="count", data=hour_data)
