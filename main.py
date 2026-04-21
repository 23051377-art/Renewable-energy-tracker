from src.extract import extract_data
from src.transform import transform_data
from src.load import load_data
from src.analyse import run_analysis

def main():
    file_name = "data/Energy_project_data1.csv"
    db_name = "database/energy.db"

    df = extract_data(file_name)
    print("Rows before transform:", len(df))

    df = transform_data(df)
    print("Rows after transform:", len(df))

    load_data(df, db_name)
    print("Data loaded successfully")

    run_analysis(db_name)

if __name__ == "__main__":
    main()