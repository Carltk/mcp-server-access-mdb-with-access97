"""
This script demonstrates how to use pandas to
query and update a Microsoft Access database using SQLAlchemy.

The script:
- Creates a new table
- Adds sample data to the table
- Prints the data in the table
- Deletes the table
"""

import shutil
import pandas as pd
import sqlalchemy as sa
from pathlib import Path
from sqlalchemy.engine import URL


def ExecuteQuery(engine: sa.Engine, query: str) -> pd.DataFrame:
    """Execute a query and return the result as a pandas dataframe"""

    with engine.begin() as conn:
        try:
            return pd.read_sql_query(sa.text(query), conn)
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            exit(1)


def ExecuteUpdate(engine: sa.Engine, query: str) -> None:
    """Execute an update query"""

    with engine.begin() as conn:
        try:
            conn.execute(sa.text(query))
        except Exception as e:
            print(f"Error executing update: {str(e)}")
            exit(1)


def SampleOperations(engine: sa.Engine) -> None:
    """Perform sample operations on the database"""

    # Create a new table in the database
    ExecuteUpdate(engine, "CREATE TABLE TestTable (ID INT, Name VARCHAR(255), Age INT)")
    print("Created TestTable")

    # add sample data to table TestTable
    ExecuteUpdate(engine, "INSERT INTO TestTable (ID, Name, Age) VALUES (1, 'John', 30)")
    ExecuteUpdate(engine, "INSERT INTO TestTable (ID, Name, Age) VALUES (2, 'Jane', 25)")
    print("Inserted data into TestTable")

    # query the table
    df = ExecuteQuery(engine, "SELECT * FROM TestTable")
    print("TestTable contents:")
    print(df)

    # delete the table
    ExecuteUpdate(engine, "DROP TABLE TestTable")
    print("Deleted TestTable")


if __name__ == "__main__":

    # creates test database
    template = Path("empty.mdb").absolute()
    dbPath = template.with_name("test.mdb").absolute()
    shutil.copy(template, dbPath)
    print(f"Test database created at")

    # create connection string for MS Access
    connection_string = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={dbPath};"
    connection_url = URL.create("access+pyodbc", query={"odbc_connect": connection_string})

    # create engine to connect to the database
    engine = sa.create_engine(connection_url)
    print("Engine created")

    # perform sample operations
    SampleOperations(engine)

    # delete the database
    engine.dispose()
    dbPath.unlink()
    print(f"Test database deleted")
