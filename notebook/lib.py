import pandas as pd
import psycopg2
from sqlalchemy import create_engine


def load_csv(file_name):
    df = pd.read_csv(f'../data/csv/{file_name}.csv')
    return df


db_params = {
    'host': 'localhost',
    'database': 'db_cloud',
    'user': 'dw',
    'password': 'dw',
    'port': '5432'
}


def write_to_dw(table_name, df):
    
    engine = create_engine(f'postgresql+psycopg2://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')

    try:
        df.to_sql(table_name, engine, schema='public', if_exists='replace', index=False)

        print(f"{table_name} written successfully!")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        engine.dispose()


def read_from_dw(table_name: str, columns: list):

    conn = psycopg2.connect(
        host=db_params['host'],
        user=db_params['user'],
        password=db_params['password'],
        port=db_params['port'],
        database=db_params['database']
    )

    cur = conn.cursor()

    # Construct the query
    query = f"SELECT {', '.join(columns)} FROM public.{table_name};"

    # Execute the query
    cur.execute(query)

    # Fetch all the rows
    rows = cur.fetchall()

    # Create a DataFrame
    df = pd.DataFrame(rows, columns=columns)

    # Close the cursor and connection
    cur.close()
    conn.close()

    return df


def read_from_dw_sql(query):

    conn = psycopg2.connect(
        host=db_params['host'],
        user=db_params['user'],
        password=db_params['password'],
        port=db_params['port'],
        database=db_params['database']
    )

    cur = conn.cursor()

    # Execute the query
    cur.execute(query)

    # Fetch all the rows
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

    # Create a DataFrame
    df = pd.DataFrame(rows, columns=columns)

    # Close the cursor and connection
    cur.close()
    conn.close()

    return df


def create_primary_key(table_name, primary_key):

    conn = psycopg2.connect(
        host=db_params['host'],
        user=db_params['user'],
        password=db_params['password'],
        port=db_params['port'],
        database=db_params['database']
    )

    cur = conn.cursor()

    # Execute the query
    cur.execute(
        f"""
        ALTER TABLE {table_name} ADD CONSTRAINT {table_name}_pk PRIMARY KEY ({primary_key});
        """
    )
    
    # Commit the changes
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()
