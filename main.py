import db_functions


def main():
    session = db_functions.connectToDB("paris")

    # Create Tables and Inserts
    # db_functions.dropAllTables(session)
    # db_functions.createTables(session)
    # db_functions.loadData(session)
    # db_functions.printAllTablesLength(session)

    # Select Queries
    # answers_df = db_functions.executeSelectQueries(session)

    # # QUERIES
    db_responses = []

    # 1o erwtima
    # db_responses.append(
    #     session.execute(
    #         "SELECT * FROM popular_recipes WHERE submitted >= '2007-01-01' AND submitted <= '2012-05-31' ALLOW FILTERING;"
    #     )
    # )
    # sort in python

    # 2o erwtima
    # db_responses.append(
    #     session.execute(
    #         "SELECT * FROM recipes_details WHERE name = 'curried bean salad' ;"
    #     )
    # )

    # 3o erwtima
    # db_responses.append(
    #     session.execute(
    #         "SELECT * FROM recipes_difficulty WHERE difficulty = 'Easy' ALLOW FILTERING;"
    #     )
    # )

    # 4o erwtima
    # db_responses.append(
    #     session.execute(
    #         "SELECT * FROM recipes_tag_rating WHERE tags CONTAINS 'course' ORDER BY avg_rating DESC, name ASC ALLOW FILTERING; "
    #     )
    # )
    # sort in python

    # 5o erwtima
    # db_responses.append(
    #     session.execute(
    #         "SELECT * FROM recipes_tag_submitted WHERE tags CONTAINS 'cocktails' ALLOW FILTERING;"
    #     )
    # )
    # sort in python

    for resp in db_responses:
        recipes_df = db_functions.loadDataIntoDataframe(resp)
        # print(f"\n{recipes_df}")
        print(recipes_df.head(20))


if __name__ == "__main__":
    main()
