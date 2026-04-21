import sqlite3
import pandas as pd
from src.queries import get_reports


def execute_query(conn, title, query, params, file):
    cursor = conn.cursor()
    cursor.execute(query, params)

    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    output = "\n" + title + "\n"

    if len(rows) == 0:
        output += "No data found\n"
    else:
        df = pd.DataFrame(rows, columns=columns)
        output += df.to_string(index=False) + "\n"

    print(output)
    file.write(output)


def run_analysis(db_name):
    conn = sqlite3.connect(db_name)

    cursor = conn.cursor()
    cursor.execute("SELECT MAX(year) FROM energy_data")
    latest_year = cursor.fetchone()[0]

    reports = get_reports(latest_year)

    # 🔥 output file
    with open("output.txt", "w", encoding="utf-8") as file:
        for title, query, params in reports:
            execute_query(conn, title, query, params, file)

    conn.close()
