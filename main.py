import pandas as pd
import db_connection


def main():
    session = db_connection.connectToDB()
    db_connection.dropAllTables(session)
    db_connection.createTables(session)
    db_connection.loadData(session)

    query = "SELECT id FROM recipes.recipes_details"
    ans = session.execute(query)._current_rows
    print(len(ans))


if __name__ == "__main__":
    main()
