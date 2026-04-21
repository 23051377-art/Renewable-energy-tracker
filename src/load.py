import sqlite3

def load_data(df, db_name):
    conn = sqlite3.connect(db_name)
    df.to_sql("energy_data", conn, if_exists="replace", index=False)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_year ON energy_data(year)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_state ON energy_data(state)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_plant_id ON energy_data(plant_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_plant_type ON energy_data(plant_type)")

    conn.commit()
    conn.close()
