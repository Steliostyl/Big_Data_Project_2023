import pandas as pd

# Defines table schema using a dictionary where key is name of the table and its value is another dictionary containing all the columns of the table along with their cassandra datatypes, primary keys and clustering keys used when creating each table.

TABLES = {
    "popular_recipes": {
        "fields": {
            "id": "int",
            "submitted": "date",
            "avg_rating": "float",
            "name": "text",
        },
        "primary_key": "PRIMARY KEY (submitted, avg_rating, id)",
        "clustering_key": "WITH CLUSTERING ORDER BY (avg_rating DESC, id ASC)",
    },
    "recipes_keywords": {
        "fields": {
            "keyword": "text",
            "avg_rating": "float",
            "name": "text",
            "id": "int",
        },
        "primary_key": "PRIMARY KEY (keyword, avg_rating, name)",
        "clustering_key": "WITH CLUSTERING ORDER BY (avg_rating DESC, name ASC)",
    },
    "recipes_difficulty": {
        "fields": {
            "difficulty": "text",
            "id": "int",
            "avg_rating": "float",
            "name": "text",
        },
        "primary_key": "PRIMARY KEY (difficulty, avg_rating, name)",
        "clustering_key": "WITH CLUSTERING ORDER BY (avg_rating DESC, name ASC)",
    },
    "recipes_tag_submitted": {
        "fields": {
            "tag": "text",
            "submitted": "date",
            "avg_rating": "float",
            "id": "int",
            "name": "text",
        },
        "primary_key": "PRIMARY KEY (tag, submitted, avg_rating, id)",
        "clustering_key": "WITH CLUSTERING ORDER BY (submitted DESC, avg_rating DESC, id ASC)",
    },
    "recipes_tag_rating": {
        "fields": {
            "tag": "text",
            "id": "int",
            "avg_rating": "float",
            "name": "text",
        },
        "primary_key": "PRIMARY KEY (tag, avg_rating, id)",
        "clustering_key": "WITH CLUSTERING ORDER BY (avg_rating DESC, id ASC)",
    },
    "recipes_details": {
        "fields": {
            "id": "int",
            "name": "text",
            "minutes": "int",
            "contributor_id": "int",
            "submitted": "date",
            "tags": "set<text>",
            "nutrition": "list<float>",
            "n_steps": "smallint",
            "steps": "list<text>",
            "description": "text",
            "ingredients": "set<text>",
            "n_ingredients": "smallint",
            "avg_rating": "float",
            "difficulty": "text",
            "keywords": "set<text>",
        },
        "primary_key": "PRIMARY KEY (name)",
        "clustering_key": "",
    },
}


def getCreateTableQuery(table_name) -> str:
    table = TABLES[table_name]
    fields_string = ", ".join(
        [f"{key} {value}" for key, value in table["fields"].items()]
    )
    return f"CREATE TABLE IF NOT EXISTS {table_name}({fields_string}, {table['primary_key']}) {table['clustering_key']};"


def getAllCreateTableQueries() -> list:
    queries = [getCreateTableQuery(t) for t in TABLES]
    indices = [
        ["keywords_index", "recipes_keywords(keywords)"],
        ["tags_index", "recipes_tag_submitted(tags)"],
        ["tags_index", "recipes_tag_rating(tags)"],
    ]
    # for index in indices:
    #     queries.append(f"CREATE INDEX IF NOT EXISTS {index[0]} ON {index[1]};")
    return queries


def getInsertQuery(table_name) -> str:
    table = TABLES[table_name]
    keys_list = list(table["fields"].keys())
    keys_string = ", ".join(keys_list)
    question_marks = ", ".join(["?" for _ in range(len(keys_list))])
    query_text = (
        f"""INSERT INTO {table_name} ({keys_string}) VALUES ({question_marks});"""
    )
    return query_text


def getAllInsertQueries(df: pd.DataFrame):
    dataframes = []
    queries = []
    for t in TABLES:
        new_df = df.copy(deep=True)
        queries.append(getInsertQuery(t))
        if t == "recipes_tag_rating" or t == "recipes_tag_submitted":
            new_df = df.explode("tags")
            new_df.rename(columns={"tags": "tag"}, inplace=True)
        elif t == "recipes_keywords":
            new_df = df.explode("keywords")
            new_df.rename(columns={"keywords": "keyword"}, inplace=True)

        new_df = new_df[[k for k in TABLES[t]["fields"].keys()]]
        dataframes.append(new_df)
    return (dataframes, queries)
