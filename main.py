import db_functions
from cassandra import ConsistencyLevel


def main():
    session = db_functions.connectToDB("paris")
    operation_mode = "select"  # Choose mode: "insert" or "select"
    consistency_levels = [
        ConsistencyLevel.TWO,
        ConsistencyLevel.QUORUM,
        ConsistencyLevel.ALL,
    ]

    select_queries = [
        "SELECT * FROM popular_recipes WHERE submitted >= '2007-01-01' AND submitted <= '2012-05-31' ALLOW FILTERING;",
        "SELECT * FROM recipes_details WHERE name = 'curried bean salad';",
        "SELECT * FROM recipes_difficulty WHERE difficulty = 'Easy';",
        "SELECT * FROM recipes_tag_submitted WHERE tag = 'course';",
        "SELECT * FROM recipes_tag_rating WHERE tag = 'course' LIMIT 20;",
    ]

    if operation_mode == "insert":
        for level in consistency_levels:
            db_functions.dropAllTables(session)
            db_functions.createTables(session)
            db_functions.insertDataWithConsistency(session, consistency_level=level)
    elif operation_mode == "select":
        # Display the queries for the user to choose
        print("Select which query to execute:")
        for i, query in enumerate(select_queries, start=1):
            print(f"{i}. {query}")

        # Get user's choice
        choice = (
            int(input("Enter the number of the query you want to execute: ")) - 1
        )  # Adjust for zero-based index
        chosen_query = [select_queries[choice]]

        # Execute the chosen query at different consistency levels
        for level in consistency_levels:
            db_functions.executeSelectQueries(
                session, consistency_level=level, queries=chosen_query
            )


if __name__ == "__main__":
    main()
