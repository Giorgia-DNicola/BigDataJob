from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import text

# Replace with your actual database URI
DATABASE_URI = 'postgresql://postgres:postgres@localhost/postgres'

try:
    # Create an engine instance
    engine = create_engine(DATABASE_URI)

    # Connect to the database
    connection = engine.connect()

    # If the connection is successful, print a success message
    print("Successfully connected to the database.")

    # SQL statements to create tables
    sql_commands = [
        text("""
        CREATE TABLE IF NOT EXISTS historical_stock_prices (
            ticker VARCHAR(255),
            name VARCHAR(255),
            sector VARCHAR(255)
        );
        """),
        text("""
        CREATE TABLE IF NOT EXISTS historical_stocks (
            ticker VARCHAR(255),
            close DOUBLE PRECISION,
            low DOUBLE PRECISION,
            high DOUBLE PRECISION,
            volume DOUBLE PRECISION,
            date TIMESTAMP
        );
        """)
    ]

    # Execute each SQL command
    for command in sql_commands:
        connection.execute(command)

    connection.commit()

    print("Tables created successfully.")

    # Close the connection
    connection.close()

except OperationalError as e:
    # Handle connection errors
    print("An error occurred:", e)

