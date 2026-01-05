"""Tools for managing database connections and data operations."""

import shutil
import typing as t
from pathlib import Path
from dataclasses import dataclass

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import URL

from fastmcp import Context
from fastmcp.exceptions import FastMCPError
from src.notes import ReadNotes



# CONNECTIONS STORAGE AND RETRIEVAL
# =================================


@dataclass
class DBConnection:
    """Dataclass to hold information about a database connection."""

    key: str            # Unique identifier for the connection
    engine: t.Any       # SQLAlchemy engine or AccessParser object
    path: str           # Path to the database file
    is_parser: bool = False  # Whether connection uses access-parser (for Access97)



def GetConnection(ctx: Context, key: str) -> DBConnection:
    """Retrieve the DBConnection object for the given key, if it exists."""

    connections = getattr(ctx.fastmcp, "connections", {})
    if key not in connections:
        raise FastMCPError(f"Not connected to the database with key '{key}'. Please use connect first.")
    return connections[key]


def GetEngine(ctx: Context, key: str) -> sa.Engine:
    """Retrieve the SQLAlchemy engine for the given key, if it exists."""
    return GetConnection(ctx, key).engine


def ListConnections(ctx: Context) -> list[dict[str, t.Any]]:
    """List all active database connections, returning key and path for each."""

    connections = getattr(ctx.fastmcp, "connections", {})
    return [{"key": conn.key, "path": conn.path} for conn in connections.values()]



# CONNECTION MANAGEMENT
# =====================



def CreateDatabase(targetPath: str, ctx: Context) -> str:
    """Create a new empty database, detect type based on extension.
    Supported extensions: .db, .sqlite, .sqlite3, .mdb, .accdb.
    """

    # Check if the target path is valid and does not already exist
    target = Path(targetPath)
    if target.exists():
        raise FastMCPError(f"Target file already exists: {target}")

    try:
        # For SQLite databases, create an empty database file
        if targetPath.endswith(".db") or targetPath.endswith(".sqlite") or targetPath.endswith(".sqlite3"):
            import sqlite3
            sqlite3.connect(targetPath)
            return f"SQLite database created at {target}"
        
        # For MS Access databases, copy the template
        elif targetPath.endswith(".mdb") or targetPath.endswith(".accdb"):

            # Ensure the empty template exists
            emptyTemplate = Path(__file__).parent.parent / "empty.mdb"
            if not emptyTemplate.exists():
                raise FastMCPError(f"MS Access empty template database not found: {emptyTemplate}")
            
            shutil.copy(str(emptyTemplate), str(target))
            return f"MS Access database created at {target}"
        
        else:
            raise FastMCPError(f"Unsupported database file extension: {targetPath}. "
                "Supported extensions: .db, .sqlite, .sqlite3, .mdb, .accdb")
            
    except Exception as e:
        raise FastMCPError(f"Failed to create database: {e}")


def Connect(key: str, ctx: Context, databasePath: str = "", readNotes: bool = False) -> str:
    """Connect to a database and store the engine under the given key, for future use.
    If readNotes is True, reads notes associated with the database (same name, with .AInotes.* suffix).
    If you already read the notes, do not read them again to go faster.
    To create a temporary in-memory database, do not specify the databasePath.
    """

    # Check if the key already exists in the engines dictionary
    connections = getattr(ctx.fastmcp, "connections")
    existing = connections.get(key)
    if existing:
        raise FastMCPError(f"Database connection with key '{key}' already exists."
            f"Existing connection: {existing.path}")

    # If no database path is specified, create an in-memory database
    # This allows us to load CSV data without writing to disk
    if databasePath == "":
        connectionUrl = "sqlite:///:memory:"
        engine = sa.create_engine(connectionUrl)
        connections[key] = DBConnection(key=key, engine=engine, path=databasePath, is_parser=False)
        message = f"Successfully connected to in-memory SQLite database with key '{key}'."
        return message

    # For Microsoft Access files, try multiple connection methods
    elif databasePath.endswith(".mdb") or databasePath.endswith(".accdb"):
        # Method 1: Try standard modern ODBC driver
        try:
            connectionString = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={databasePath};"
            connectionUrl = URL.create("access+pyodbc", query={"odbc_connect": connectionString})
            engine = sa.create_engine(connectionUrl)
            # test the connection
            with engine.connect() as conn:
                conn.execute(sa.text("SELECT 1"))
            connections[key] = DBConnection(key=key, engine=engine, path=databasePath, is_parser=False)
            message = f"Successfully connected to database with key '{key}'."
        except Exception as modern_error:
            # Method 2: Try legacy ODBC driver for Access97
            if databasePath.endswith(".mdb"):
                try:
                    connectionString = f"DRIVER={{Microsoft Access Driver (*.mdb)}};DBQ={databasePath};"
                    connectionUrl = URL.create("access+pyodbc", query={"odbc_connect": connectionString})
                    engine = sa.create_engine(connectionUrl)
                    # test the connection
                    with engine.connect() as conn:
                        conn.execute(sa.text("SELECT 1"))
                    connections[key] = DBConnection(key=key, engine=engine, path=databasePath, is_parser=False)
                    message = f"Successfully connected to database with key '{key}' (using legacy driver)."
                except Exception as legacy_error:
                    # Method 3: Try access-parser for Access97
                    try:
                        from access_parser import AccessParser
                        import warnings
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore")  # Suppress warning messages
                            conn = AccessParser(databasePath)
                        connections[key] = DBConnection(key=key, engine=conn, path=databasePath, is_parser=True)
                        message = f"Successfully connected to database with key '{key}' (using access-parser for Access97)."
                    except Exception as parser_error:
                        # All methods failed, combine error messages
                        raise FastMCPError(
                            f"Could not connect to database. Tried:\n"
                            f"  1. Modern ODBC driver: {modern_error}\n"
                            f"  2. Legacy ODBC driver: {legacy_error}\n"
                            f"  3. access-parser (Access97): {parser_error}"
                        )
            else:
                # Modern driver failed for .accdb file (not Access97)
                raise modern_error

    # For SQLite files, use sqlite:/// connection string
    elif databasePath.endswith(".db") or databasePath.endswith(".sqlite") or databasePath.endswith(".sqlite3"):
        connectionUrl = f"sqlite:///{databasePath}"
        engine = sa.create_engine(connectionUrl)
        # test the connection
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
        connections[key] = DBConnection(key=key, engine=engine, path=databasePath, is_parser=False)
        message = f"Successfully connected to database with key '{key}'."

    # Handle other unknown file types
    else:
        raise FastMCPError(f"Unsupported database file extension: {databasePath}")
    
    # read notes associated with the database
    if readNotes:
        try:
            notes = ReadNotes(databasePath)
            message += f"\nNotes: {notes}"
        except FastMCPError as e:
            message += f"\nError reading notes: {e}"
    
    return message


def Disconnect(key: str, ctx: Context) -> str:
    """Disconnect from the MS Access database identified by key."""

    # Ensure the connection exists
    connections = getattr(ctx.fastmcp, "connections", {})
    if key not in connections:
        raise FastMCPError(f"No active database connection with key '{key}' to disconnect.")

    conn_obj = connections[key]

    # Handle different connection types
    if conn_obj.is_parser:
        # AccessParser doesn't need explicit cleanup
        pass
    else:
        # Dispose of SQLAlchemy engine
        conn_obj.engine.dispose()

    del connections[key]
    return f"Disconnected from the database with key '{key}'."



# DATA MANAGEMENT
# ===============


def Query(key: str, sql: str, ctx: Context, params: dict[str, t.Any] = {}) -> list[dict]:
    """Execute a SELECT query on the database identified by key and return results as a list of records.
    Use backticks to escape table and column names.
    ALWAYS insert named parameters (:param_name) in the SQL query to avoid SQL injection.
    Pass a dictionary as params to provide values for the SQL query.
    Before executing a query, make sure to know the record count, using SELECT TOP (Access)
    or LIMIT (SQLite) to limit the number of records returned and avoid large responses.

    IMPORTANT FOR MS ACCESS ONLY:
    Do not use this tool to discover existing tables or query system objects or schema.
    Instead, ask the user about existing tables, their purpose, structure and content.
    To discover the structure of a table, use SELECT TOP 1 * FROM <table_name>.
    """

    conn_obj = GetConnection(ctx, key)

    # Check if this is an AccessParser connection (for Access97)
    if conn_obj.is_parser:
        # AccessParser doesn't support SQL queries directly
        # Extract table name from SQL (simple parsing for SELECT * FROM table)
        import re
        match = re.search(r"FROM\s+\[?(\w+)\]?", sql, re.IGNORECASE)
        if not match:
            raise FastMCPError("AccessParser only supports simple SELECT * FROM table queries without WHERE, JOIN, etc.")

        table_name = match.group(1)

        # Parse the table
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            table_data = conn_obj.engine.parse_table(table_name)

        # Convert to list of dicts
        if not table_data or len(table_data) == 0:
            return []

        # AccessParser returns data as a defaultdict where keys are column names
        # and values are lists of column data
        # Format: {'Column1': [val1, val2, ...], 'Column2': [val1, val2, ...], ...}
        columns = list(table_data.keys())
        if not columns:
            return []

        # Get the number of rows from the first column
        num_rows = len(table_data[columns[0]])

        # Convert to list of dictionaries (row-oriented)
        rows = []
        for row_idx in range(num_rows):
            row_dict = {}
            for column in columns:
                col_data = table_data[column]
                row_dict[column] = col_data[row_idx] if row_idx < len(col_data) else None
            rows.append(row_dict)

        return rows
    else:
        # Use pandas to execute query and convert results to dict format
        # This automatically handles proper data type conversion
        with conn_obj.engine.begin() as conn:
            df = pd.read_sql_query(sa.text(sql), conn, params=params)
            return df.to_dict("records")


def Update(key: str, sql: str, ctx: Context, params: list[dict[str, t.Any]] = []) -> bool:
    """Execute an UPDATE/INSERT/DELETE statement on the database identified by key.
    Use backticks to escape table and column names.
    ALWAYS insert named parameters (:param_name) in the SQL statement to avoid SQL injection.
    Pass a list of dictionaries as params to provide values for the SQL statement.
    The tool will repeat the statement execution for each dictionary in the list.
    If one statement fails, the entire transaction will be rolled back.
    """

    conn_obj = GetConnection(ctx, key)

    # Check if this is an AccessParser connection (for Access97)
    if conn_obj.is_parser:
        # AccessParser is read-only - cannot update Access97 databases
        raise FastMCPError("Cannot modify Access97 databases using access-parser (read-only). You would need to convert the database to a newer format using Microsoft Access.")
    else:
        # Execute the update in a transaction
        # SQLAlchemy automatically commits if no errors occur
        with conn_obj.engine.begin() as conn:
            conn.execute(sa.text(sql), parameters=params)
            return True
