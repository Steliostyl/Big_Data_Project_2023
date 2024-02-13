import db_functions


def main():
    session = db_functions.connectToDB("paris")
    db_functions.printAllTablesLength(session)


if __name__ == "__main__":
    main()
