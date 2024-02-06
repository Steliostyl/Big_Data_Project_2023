import pandas as pd
import db_connection


def main():
    # recipes_df = pd.read_csv(DATASET_PATH + "RAW_recipes.csv")
    # print(recipes_df["ingredients"])
    session = db_connection.connectToDB()
    db_connection.createTables(session)
    return


if __name__ == "__main__":
    main()
