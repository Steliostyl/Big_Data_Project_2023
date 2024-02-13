import db_functions


def main():
    session = db_functions.connectToDB("stelios")
    # db_functions.printAllTablesLength(session)
    answers_df = db_functions.executeSelectQueries(session)
    print(answers_df[0].head(20))


if __name__ == "__main__":
    main()
