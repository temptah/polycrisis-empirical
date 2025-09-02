import os
from dotenv import load_dotenv
import sqlalchemy as sa
import pandas as pd

load_dotenv()

url = sa.URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT")),
    database=os.getenv("DB_NAME"),
)

engine = sa.create_engine(url, future=True)

with engine.connect() as con:
    # List schemas we created
    schemas = con.exec_driver_sql("""
        SELECT schema_name FROM information_schema.schemata
        WHERE schema_name IN ('meta','raw','feat','model','outputs')
        ORDER BY schema_name;
    """).fetchall()
    print("Schemas:", [s[0] for s in schemas])

    # List tables
    tables = con.exec_driver_sql("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema IN ('meta','raw','feat','model','outputs')
        ORDER BY 1, 2;
    """).fetchall()
    df = pd.DataFrame(tables, columns=["schema","table"])
    print(df)
