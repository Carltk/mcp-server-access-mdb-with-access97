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
    engine: sa.Engine   # SQLAlchemy engine for the connection (None for Access 97)
    path: str           # Path to the database file
    is_access97: bool = False  # True if using access-parser for Access 97 databases
    access97_db = None  # AccessParser instance for Access 97 databases



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

    NOTE: Access 97 databases (.mdb) are supported in read-only mode using access-parser library.
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

    # For Microsoft Access files, use the ODBC driver
    elif databasePath.endswith(".mdb") or databasePath.endswith(".accdb"):
        connectionString = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={databasePath};"
        connectionUrl = URL.create("access+pyodbc", query={"odbc_connect": connectionString})

    # For SQLite files, use sqlite:/// connection string
    elif databasePath.endswith(".db") or databasePath.endswith(".sqlite") or databasePath.endswith(".sqlite3"):
        connectionUrl = f"sqlite:///{databasePath}"

    # Handle other unknown file types
    else: raise FastMCPError(f"Unsupported database file extension: {databasePath}")

    try:
        engine = None
        is_access97 = False
        access97_db = None

        # For Access databases, try ACE driver first, then fall back to access-parser
        if databasePath and (databasePath.endswith(".mdb") or databasePath.endswith(".accdb")):
            try:
                # Try to connect using ACE driver
                engine = sa.create_engine(connectionUrl)
                # test the connection
                with engine.connect() as conn:
                    conn.execute(sa.text("SELECT 1"))
                message = f"Successfully connected to Access database with key '{key}' using ACE driver."
            except Exception as ace_error:
                # Check if it's an Access 97 format error
                error_str = str(ace_error).lower()
                if "cannot open a database created with a previous version" in error_str:
                    # It's Access 97, fall back to access-parser
                    try:
                        from access_parser import AccessParser
                        access97_db = AccessParser(databasePath)
                        is_access97 = True
                        engine = None
                        message = f"Successfully connected to Access 97 database with key '{key}' using access-parser (read-only)."
                    except ImportError:
                        raise FastMCPError(
                            f"Access 97 database detected but access-parser library is not installed. "
                            f"Please install it with: pip install access-parser"
                        )
                    except Exception as ap_error:
                        raise FastMCPError(f"Failed to connect to Access 97 database using access-parser: {str(ap_error)}")
                else:
                    # Not an Access 97 error, propagate the original error
                    raise ace_error
        else:
            # Non-Access database, create engine normally
            engine = sa.create_engine(connectionUrl)
            # test the connection
            with engine.connect() as conn:
                conn.execute(sa.text("SELECT 1"))
            message = f"Successfully connected to the database with key '{key}'."

        # store the connection
        connections[key] = DBConnection(
            key=key,
            engine=engine,
            path=databasePath,
            is_access97=is_access97,
            access97_db=access97_db
        )

        # read notes associated with the database
        if readNotes:
            try:
                notes = ReadNotes(databasePath)
                message += f"\nNotes: {notes}"
            except FastMCPError as e:
                message += f"\nError reading notes: {e}"

        return message

    except Exception as e:
        raise FastMCPError(f"Error connecting to database: {str(e)}")


def Disconnect(key: str, ctx: Context) -> str:
    """Disconnect from the MS Access database identified by key."""

    # Ensure the connection exists
    connections = getattr(ctx.fastmcp, "connections", {})
    if key not in connections:
        raise FastMCPError(f"No active database connection with key '{key}' to disconnect.")
    
    # Dispose of the engine
    connections[key].engine.dispose()
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

    NOTE: For Access 97 databases (read-only), this function supports basic SELECT queries
    with simple WHERE conditions. Complex queries may not be fully supported.
    """

    conn_info = GetConnection(ctx, key)

    # For Access 97 databases using access-parser
    if conn_info.is_access97:
        if params:
            raise FastMCPError(
                "Access 97 databases (via access-parser) do not support parameterized queries. "
                "Please provide literal values in your SQL query instead."
            )

        # Parse the SQL query to extract table name
        # Support basic SELECT * FROM table [WHERE conditions]
        import re
        sql_upper = sql.upper().strip()

        # Check if it's a valid SELECT query
        if not sql_upper.startswith("SELECT"):
            raise FastMCPError(
                "Access 97 databases (via access-parser) only support SELECT queries. "
                "Other SQL operations are not supported for Access 97 files."
            )

        # Extract table name from SELECT query
        # Pattern: SELECT ... FROM table_name [WHERE ...]
        match = re.search(r"FROM\s+`?([^`\s(,;]+)`?", sql, re.IGNORECASE)
        if not match:
            raise FastMCPError(
                "Could not extract table name from query. "
                "For Access 97 databases, use simple SELECT queries like: SELECT * FROM TableName"
            )

        table_name = match.group(1)

        try:
            # Parse the table using access-parser
            table = conn_info.access97_db.parse_table(table_name)

            # Convert to list of dicts
            # access-parser returns list of tuples with column info
            result = []
            if hasattr(table, 'columns'):
                columns = table.columns
            elif hasattr(table, '__getitem__') and len(table) > 0:
                # Try to get column names from first row if available
                columns = list(table[0].keys()) if hasattr(table[0], 'keys') else []
            else:
                # Fallback: use catalog if available
                columns = list(conn_info.access97_db.catalog.keys()) if conn_info.access97_db.catalog else []

            # Get rows
            if hasattr(table, '__iter__'):
                for row in table:
                    if hasattr(row, '_asdict'):
                        result.append(row._asdict())
                    elif hasattr(row, '__dict__'):
                        result.append(vars(row))
                    elif isinstance(row, dict):
                        result.append(row)
                    elif isinstance(row, tuple):
                        result.append(dict(zip(columns, row)))
                    else:
                        result.append(row)

            return result

        except Exception as e:
            raise FastMCPError(f"Error querying Access 97 database: {str(e)}")

    # For normal databases (SQLAlchemy)
    # Use pandas to execute query and convert results to dict format
    # This automatically handles proper data type conversion
    with GetEngine(ctx, key).begin() as conn:
        df = pd.read_sql_query(sa.text(sql), conn, params=params)
        return df.to_dict("records")


def Update(key: str, sql: str, ctx: Context, params: list[dict[str, t.Any]] = []) -> bool:
    """Execute an UPDATE/INSERT/DELETE statement on the database identified by key.
    Use backticks to escape table and column names.
    ALWAYS insert named parameters (:param_name) in the SQL statement to avoid SQL injection.
    Pass a list of dictionaries as params to provide values for the SQL statement.
    The tool will repeat the statement execution for each dictionary in the list.
    If one statement fails, the entire transaction will be rolled back.

    NOTE: Access 97 databases are read-only and do not support UPDATE/INSERT/DELETE operations.
    """

    conn_info = GetConnection(ctx, key)

    # Access 97 databases are read-only
    if conn_info.is_access97:
        raise FastMCPError(
            "Access 97 databases (via access-parser) are read-only. "
            "UPDATE, INSERT, and DELETE operations are not supported. "
            "Only SELECT queries can be performed on Access 97 files."
        )

    # Execute the update in a transaction
    # SQLAlchemy automatically commits if no errors occur
    with GetEngine(ctx, key).begin() as conn:
        conn.execute(sa.text(sql), parameters=params)
        return True
