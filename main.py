import db_functions
from cassandra import ConsistencyLevel
import matplotlib.pyplot as plt
from pprint import pprint
import pandas as pd


def main():
    session = db_functions.connectToDB("paris")
    operation_mode = "insert"  # Choose mode: "insert" or "select" or "select-benchmark"
    consistency_levels = [
        ConsistencyLevel.TWO,
        ConsistencyLevel.QUORUM,
        ConsistencyLevel.ALL,
    ]
    level_names = ["ALL", "QUORUM", "TWO"]
    average_insert_times = []
    average_select_times = {level: [] for level in consistency_levels}

    select_queries = [
        "SELECT * FROM popular_recipes WHERE submitted >= '2007-01-01' AND submitted <= '2012-05-31' ALLOW FILTERING;",
        "SELECT * FROM recipes_details WHERE name = 'curried bean salad';",
        "SELECT * FROM recipes_difficulty WHERE difficulty = 'Easy';",
        "SELECT * FROM recipes_tag_submitted WHERE tag = 'course';",
        "SELECT * FROM recipes_tag_rating WHERE tag = 'course' LIMIT 20;",
    ]

    if operation_mode == "insert":
        db_functions.dropAllTables(session)
        db_functions.createTables(session)
        df_data = []
        for level in consistency_levels:
            for iteration in range(1, 6):
                time = db_functions.insertDataWithConsistency(session, level)
                df_data.append([level, iteration, time])
            # average_insert_times.append(avg_time)

        times_df = pd.DataFrame(
            data=df_data, columns=["Consistency_Level", "Iteration", "Time"]
        )
        times_df.to_excel("raw_inserts.xlsx", index=False)

        print("Completed insert operations at all consistency levels.\n")

        # Plotting the results for insert operations
        fig, ax = plt.subplots()
        ax.bar(level_names, average_insert_times, color=["blue", "orange", "green"])

        ax.set_ylabel("Avg. Time (s)")
        ax.set_title("Average Insert Time by Consistency Level")
        ax.set_xticks(range(len(level_names)))
        ax.set_xticklabels(level_names)

        # Add data labels
        for i in ax.patches:
            ax.text(
                i.get_x() + i.get_width() / 2,
                i.get_height() + 0.5,
                str(round(i.get_height(), 2)),
                fontsize=11,
                color="black",
                ha="center",
            )

        plt.show()

    elif operation_mode == "select-benchmark":
        df = None
        for idx, query in enumerate(select_queries):
            print(f"Query: {query}")
            for level in consistency_levels:
                avg_time, df = db_functions.executeSelectQueriesv2(
                    session,
                    consistency_level=level,
                    queries=[query],
                    df=df,
                    query_idx=idx,
                )
                average_select_times[level].append(avg_time)
            print("Completed query execution at all consistency levels.\n")
        df.to_excel("raw_data.xlsx", index=False)

        # Plotting the results
        labels = ["Query 1", "Query 2", "Query 3", "Query 4", "Query 5"]
        x = range(len(labels))  # the label locations
        width = 0.15  # the width of the bars, adjusted for the number of bars
        levelName = ["TWO", "QUORUM", "ALL"]

        fig, ax = plt.subplots()
        for i, level in enumerate(consistency_levels):
            ax.bar(
                [p + i * width for p in x],
                average_select_times[level],
                width,
                label=levelName[i],
            )

        # Add some text for labels, title, and custom x-axis tick labels, etc.
        ax.set_ylabel("Avg. Time (s)")
        ax.set_title("Avg. Execution Time by Query and Consistency Level")
        ax.set_xticks([p + width * 1.5 for p in x])
        ax.set_xticklabels(labels)
        ax.legend()

        fig.tight_layout()
        plt.show()

    elif operation_mode == "select":
        for i in range(1, len(select_queries)):
            print(select_queries[i])
            answer = db_functions.loadDataIntoDataframe(
                session.execute(select_queries[i])
            )
            if i == 0:
                print(
                    answer.sort_values(by="avg_rating", ascending=False)
                    .head(30)
                    .to_string(index=False)
                )
            elif i == 1:
                pprint(answer.loc[0].to_dict())
            else:
                print(answer.head(30))
            print()
        exit()


if __name__ == "__main__":
    main()
