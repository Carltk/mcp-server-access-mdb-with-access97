"""This scripts identifies the type of database in a file."""

import sqlite3
from pathlib import Path
import sqlalchemy as sa

from scouting_mdb import SampleOperations  


def CheckSQLite3(path: str) -> bool:
    """Check if the file is a SQLite3 database."""
    with open(path, 'rb') as f:
        header = f.read(100)
    return b'SQLite format 3' in header


if __name__ == "__main__":

    dbPath = Path("test.db").absolute()

    # create the database
    sqlite3.connect(dbPath).close()
    print(f"Test database created")

    # check if the database is a SQLite3 database
    print(f"Is SQLite3 database: {CheckSQLite3(str(dbPath))}")

    # connects using sqlalchemy
    connectionUrl = f"sqlite:///{dbPath}"
    engine = sa.create_engine(connectionUrl)
    print(f"Connected to SQLite3")

    # perform sample operations
    SampleOperations(engine)

    # delete the database
    engine.dispose()
    dbPath.unlink()
    print(f"Test database deleted")
