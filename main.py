import pandas as pd
import db_connection


def main():
    session = db_connection.connectToDB()
    query = "SELECT * FROM recipes.recipes_details"
    db_connection.createTables(session)
    db_connection.loadData(session)
    return


if __name__ == "__main__":
    main()
