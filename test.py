import pandas as pd

df = pd.read_csv("dataset/RAW_recipes.csv")
missing_values_per_column = df.isna().sum()

print(df[["submitted", "name", "id"]])
