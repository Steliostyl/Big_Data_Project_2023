import pandas as pd

DATASET_PATH = "dataset/"


def split_name(name):
    return name.split()


def assign_difficulty(row, tertiles):
    if row["minutes"] <= tertiles[0]:
        return "Easy"
    elif row["minutes"] <= tertiles[1]:
        return "Medium"
    else:
        return "Hard"


recipes_df = pd.read_csv(DATASET_PATH + "RAW_recipes.csv")
interactions_df = pd.read_csv(DATASET_PATH + "RAW_interactions.csv")

# Fill NaN values in name and description columns
recipes_df.fillna({"name": "", "description": ""}, inplace=True)

# Split name into keywords
recipes_df["keywords"] = recipes_df["name"].apply(split_name)

# Assign difficulty based on tertiles
tertiles = recipes_df["minutes"].quantile([1 / 3, 2 / 3]).tolist()
recipes_df["difficulty"] = recipes_df.apply(
    lambda row: assign_difficulty(row, tertiles), axis=1
)

merged_df = pd.merge(
    recipes_df,
    interactions_df[["recipe_id", "rating"]]
    .groupby(by="recipe_id")
    .mean()
    .rename(columns={"rating": "avg_rating"}),
    left_on="id",
    right_on="recipe_id",
)

print(merged_df.head(10))

# Save the merged_df DataFrame to a CSV file
merged_df.to_csv((DATASET_PATH + "merged_recipes.csv"), index=False)
