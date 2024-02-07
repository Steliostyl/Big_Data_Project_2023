import pandas as pd
import db_connection


def main():
    session = db_connection.connectToDB()
    # query = "SELECT id FROM recipes.recipes_details"
    # ans = session.execute(query)._current_rows
    # print(len(ans))
    db_connection.createTables(session)
    return


if __name__ == "__main__":
    main()
