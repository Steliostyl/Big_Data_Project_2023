from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
import json
import pandas as pd
import ast
import time

CREDENTIALS_PATH = "credentials/"
DATASET_PATH = "dataset/"


def connectToDB():
    # This secure connect bundle is autogenerated when you download your SCB,
    # if yours is different update the file name below
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
        return session
    else:
        print("An error occurred.")
        return -1


def createTables(session: Session):

    create_popular_recipes = """
        CREATE TABLE IF NOT EXISTS recipes.popular_recipes(
            submitted date,
            avg_rating float,
            name text,
            id int,
            PRIMARY KEY (submitted, avg_rating)
        ) WITH CLUSTERING ORDER BY (avg_rating DESC);
    """
    create_recipes_by_keyword = """
        CREATE TABLE IF NOT EXISTS recipes.recipes_keyword(
            keywords list<text>,
            avg_rating float,
            name text,
            id int,
            PRIMARY KEY (keywords, avg_rating)
        ) WITH CLUSTERING ORDER BY (avg_rating DESC);
    """

    create_recipes_by_difficulty = """
        CREATE TABLE IF NOT EXISTS recipes.recipes_difficulty(
            difficulty text,
            avg_rating float,
            name text,
            id int,
            PRIMARY KEY (difficulty, avg_rating)
        ) WITH CLUSTERING ORDER BY (avg_rating DESC);
    """

    create_recipes_by_tag_submitted = """
        CREATE TABLE IF NOT EXISTS recipes.recipes_tag_submitted(
            tag text,
            submitted date,
            avg_rating float,
            name text,
            id int,
            PRIMARY KEY (tag, submitted, avg_rating)
        ) WITH CLUSTERING ORDER BY (submitted DESC, avg_rating DESC);
    """

    create_recipes_by_tag_rating = """
        CREATE TABLE IF NOT EXISTS recipes.recipes_tag_rating(
            avg_rating float,
            name text,
            id int,
            PRIMARY KEY (submitted, avg_rating)
        ) WITH CLUSTERING ORDER BY (avg_rating DESC);
    """
    create_recipes_details = """
        CREATE TABLE IF NOT EXISTS recipes.recipes_details(
            id int,
            name text,
            minutes int,
            contributor_id int,
            submitted date,
            tags list<text>,
            nutrition list<float>,
            n_steps smallint,
            steps list<text>,
            description text,
            ingredients list<text>,
            n_ingredients smallint,
            avg_rating float,
            difficulty text,
            keywords list<text>
            PRIMARY KEY (id)
        );
    """

    session.execute(create_popular_recipes)
    session.execute(create_recipes_by_keyword)
    session.execute(create_recipes_by_difficulty)
    session.execute(create_recipes_by_tag_submitted)
    session.execute(create_recipes_by_tag_rating)
    session.execute(create_recipes_details)


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
    queries = [
        ["popular_recipes", "submitted, avg_rating, name, id", 4],
        ["recipes_keyword", "keywords, avg_rating, name, id", 4],
        ["recipes_difficulty", "difficulty, avg_rating, name, id", 4],
        ["recipes_by_tag_submitted", "tag, submitted, avg_rating, name, id", 5],
        ["recipes_by_tag_rating", "tag, avg_rating, name, id", 4],
        [
            "recipes_details",
            (
                "name, id, minutes, contributor_id, submitted, tags, nutrition, n_steps,"
                + "steps, description, ingredients, n_ingredients, avg_rating,"
                + "difficulty, keywords"
            ),
            15,
        ],
    ]
    for query in queries:
        # Create a string with the desired number of question marks
        question_marks = ", ".join(["?" for _ in range(query[2])])
        query_text = f"""INSERT INTO recipes.{query[0]} ({query[1]}) VALUES ({question_marks});"""
        print(query_text)

        insert_query = session.prepare(query_text)

        total_rows = len(df)
        start_time = time.time()
        for idx, row in enumerate(df.itertuples(index=False), start=1):
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
