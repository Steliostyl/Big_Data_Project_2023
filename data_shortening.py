import pandas as pd

# Path to your dataset
DATASET_PATH = "dataset/"

# Load the RAW_recipes.csv dataset
recipes_df = pd.read_csv(DATASET_PATH + "RAW_recipes.csv")

# Select 1,000 random records from recipes_df
sample_recipes_df = recipes_df.sample(
    n=1_000, random_state=42
)  # Set a random state for reproducibility

# Save the sample to a new CSV file
sample_recipes_df.to_csv(DATASET_PATH + "sample_RAW_recipes.csv", index=False)

# Now, load the RAW_interactions.csv dataset
interactions_df = pd.read_csv(DATASET_PATH + "RAW_interactions.csv")

# Get the list of recipe IDs from the sample recipes
sample_recipe_ids = sample_recipes_df["id"].unique()

# Filter interactions to keep only those with a recipe_id in the sample recipe IDs
sample_interactions_df = interactions_df[
    interactions_df["recipe_id"].isin(sample_recipe_ids)
]

# Save the filtered interactions to a new CSV file
sample_interactions_df.to_csv(DATASET_PATH + "sample_RAW_interactions.csv", index=False)
