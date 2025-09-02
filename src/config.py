import os
from dotenv import load_dotenv
import sqlalchemy as sa

def make_engine():
    load_dotenv()
    url = sa.URL.create(
        "postgresql+psycopg2",
        username=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "postgres"),
    )
    return sa.create_engine(url, future=True)
