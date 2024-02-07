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
        "secure_connect_bundle": CREDENTIALS_PATH + "secure-connect-big-data-2023.zip"
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
            PRIMARY KEY (id, avg_rating)
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
            PRIMARY KEY (id, submitted, avg_rating)
        ) WITH CLUSTERING ORDER BY (submitted DESC, avg_rating DESC);
    """

    create_recipes_by_tag_rating = """
        CREATE TABLE IF NOT EXISTS recipes.recipes_tag_rating(
            avg_rating float,
            name text,
            id int,
            PRIMARY KEY (id, avg_rating)
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
            keywords list<text>,
            PRIMARY KEY (id)
        );
    """

    session.execute(create_popular_recipes)
    session.execute(create_recipes_by_keyword)
    session.execute(create_recipes_by_difficulty)
    session.execute(create_recipes_by_tag_submitted)
    session.execute(create_recipes_by_tag_rating)
    session.execute(create_recipes_details)
