import db_functions


def main():
    session = db_functions.connectToDB("stelios")
    # db_functions.printAllTablesLength(session)
    # answers_df = db_functions.executeSelectQueries(session)

    # QUERIES
    queries = [f"SELECT * FROM popular_recipes", f"SELECT * FROM popular_recipes"]
    for query in queries:
        recipes_df = db_functions.loadDataIntoDataframe(session.execute(query))
        print(f"\n{query}")
        print(recipes_df.head(20))


if __name__ == "__main__":
    main()
