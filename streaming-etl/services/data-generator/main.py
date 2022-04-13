import psycopg2
import os


def main():
    host = os.getenv("POSTGRES_HOST")
    database = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")

    db_connection = psycopg2.connect(
        host=host, database=database, user=user, password=password
    )

    with db_connection.cursor() as cur:
        cur.execute("SELECT version()")
        db_version = cur.fetchone()

        print(f"--- postgres version: {db_version} ---")


if __name__ == "__main__":
    main()
