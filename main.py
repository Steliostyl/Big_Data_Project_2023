import pandas as pd
import db_connection


def main():
    session = db_connection.connectToDB()
    db_connection.printAllTablesLength(session)
    # db_connection.dropAllTables(session)
    # db_connection.createTables(session)
    # db_connection.loadData(session)


if __name__ == "__main__":
    main()
