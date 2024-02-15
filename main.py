import db_functions


def main():
    session = db_functions.connectToDB("stelios")
    # db_functions.printAllTablesLength(session)
    # answers_df = db_functions.executeSelectQueries(session)

    # QUERIES
    db_responses = []
    db_responses.append(session.execute("SELECT * FROM popular_recipes"))
    db_responses.append(session.execute("SELECT * FROM popular_recipes"))
    db_responses.append(session.execute("SELECT * FROM popular_recipes"))
    db_responses.append(session.execute("SELECT * FROM popular_recipes"))

    for resp in db_responses:
        recipes_df = db_functions.loadDataIntoDataframe(resp)
        print(f"\n{recipes_df}")
        print(recipes_df.head(20))


if __name__ == "__main__":
    main()
