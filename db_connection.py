from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
import json
import pandas as pd
import ast
import time
import query_generation

CREDENTIALS_PATH = "credentials/"
DATASET_PATH = "dataset/"
SAT_QUERY = (
    "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'recipes'"
)


def connectToDB():
    cloud_config = {
        "secure_connect_bundle": CREDENTIALS_PATH
        + "secure-connect-big-data-project-db.zip"
    }

    # This token JSON file is autogenerated when you download your token,
    # if yours is different update the file name below
    with open(CREDENTIALS_PATH + "big_data_project_db-token.json") as f:
        secrets = json.load(f)

    CLIENT_ID = secrets["clientId"]
    CLIENT_SECRET = secrets["secret"]

    auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()

    row = session.execute("select release_version from system.local").one()
    if row:
        print("Connection successful!")
        session.set_keyspace("recipes")
        return session
    else:
        print("An error occurred.")
        return -1


def createTables(session: Session):
    """Creates all tables in the database."""

    create_table_queries = query_generation.getAllCreateTableQueries()
    [session.execute(q) for q in create_table_queries]
    print("Created tables!")


def split_name(name):
    return name.split()


def assign_difficulty(row, tertiles):
    if row["minutes"] <= tertiles[0]:
        return "Easy"
    elif row["minutes"] <= tertiles[1]:
        return "Medium"
    else:
        return "Hard"


def mergeDataframes():
    recipes_df = pd.read_csv(DATASET_PATH + "RAW_recipes.csv")
    interactions_df = pd.read_csv(DATASET_PATH + "RAW_interactions.csv")
    recipes_df.fillna({"name": "", "description": ""}, inplace=True)

    # Convert string representations of lists back to actual lists
    for column in recipes_df.columns:
        try:
            recipes_df[column] = recipes_df[column].apply(ast.literal_eval)
        except (ValueError, SyntaxError):
            pass  # Skip columns that cannot be converted to lists

    recipes_df["keywords"] = recipes_df["name"].apply(split_name)

    tertiles = recipes_df["minutes"].quantile([1 / 3, 2 / 3]).tolist()

    # Assign difficulty based on tertiles-
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
    return merged_df


def loadData(session: Session):
    df = mergeDataframes()
    (fields, queries) = query_generation.getAllInsertQueries()

    for index, query in enumerate(queries):
        print(query)
        insert_query = session.prepare(query)
        current_fields = fields[index]
        total_rows = len(df)
        start_time = time.time()
        values = df[current_fields].values.tolist()
        for idx, row in enumerate(values, start=1):
            session.execute(insert_query, row)
            # Print out the progress and estimated time of completion
            if idx % 100 == 0:
                elapsed_time = time.time() - start_time
                rows_per_second = idx / elapsed_time
                estimated_total_time = total_rows / rows_per_second
                remaining_time = estimated_total_time - elapsed_time
                print(
                    f"Inserted {idx} of {total_rows} records ({(idx/total_rows)*100:.2f}%)"
                )
                print(f"Estimated time remaining: {remaining_time/60:.2f} minutes")
    print("Data loading complete.")


def executeSelectQueries(session: Session):
    # Example keywords
    keywords = ["tasty", "amazing", "pasta"]
    # keywords = input("Input keywords seperated by commas").split(",")
    keywords_query = """SELECT * FROM recipes_keywords WHERE keywords CONTAINS IN ?"""

    rows = session.execute(keywords_query, (keywords,))
    return rows


def dropAllTables(session: Session):
    result = session.execute(SAT_QUERY)
    tables = [row.table_name for row in result]

    # Drop each table
    for table in tables:
        drop_query = f"DROP TABLE IF EXISTS recipes.{table}"
        session.execute(drop_query)
        print(f"Dropped table: {table}")


def printAllTablesLength(session: Session):
    result = session.execute(SAT_QUERY)
    tables = [row.table_name for row in result]
    for table in tables:
        select_query = f"SELECT id FROM {table}"
        ans = session.execute(select_query)._current_rows
        print(f"Table {table} has {len(ans)} rows.")
