#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Important: This script is for Python 2.7

import cx_Oracle
import time
import os

# --- 1. CONFIGURE YOUR DATABASE CONNECTION DETAILS HERE ---
# It's best practice to get these from environment variables or a config file,
# but we will define them here for simplicity.

DB_USERNAME = "your_username"
DB_PASSWORD = "your_password"
DB_HOST = "dbserver.example.com"  # or an IP address
DB_PORT = 1521
DB_SERVICE_NAME = "ORCLPDB1"


sql_query = "SELECT COUNT(*) FROM QPID"



if 'LD_LIBRARY_PATH' not in os.environ:
    print "WARNING: LD_LIBRARY_PATH is not set."
    print "The script might fail if Oracle Instant Client is not in a system-default location."
    print "Please run 'export LD_LIBRARY_PATH=/path/to/your/instantclient'"



dsn = cx_Oracle.makedsn(DB_HOST, DB_PORT, service_name=DB_SERVICE_NAME)

connection = None # Initialize connection to None
try:
    print "Connecting to the database..."

    # Record the start time (includes connection time)
    start_time = time.time()

    # Establish the connection to the database
    connection = cx_Oracle.connect(
        user=DB_USERNAME,
        password=DB_PASSWORD,
        dsn=dsn
    )

    print "Connection successful."

    # Create a cursor object to execute SQL
    cursor = connection.cursor()

    print "Executing query: " + sql_query
    cursor.execute(sql_query)

    # Fetch the result. use fetchone() for a single result,
    # or fetchall() for multiple rows.
    result = cursor.fetchone()

    # Record the end time
    end_time = time.time()

    # Calculate the duration
    duration = end_time - start_time

    print "\n--- Query Results ---"
    if result:
        # result is a tuple, e.g., (2600000,) so we access the first element
        print "Row count: %d" % result[0]
    else:
        print "Query returned no results."

    print "\n--- Performance ---"
    print "Total execution time (including connection): %.2f seconds" % duration


except cx_Oracle.DatabaseError as e:
    # This block will catch any Oracle-specific errors
    error, = e.args
    print "Oracle Database Error:"
    print "Error Code:", error.code
    print "Error Message:", error.message

finally:
    # This block will always run, whether there was an error or not.
    # It's crucial for closing the connection to free up database resources.
    if connection:
        print "\nClosing the database connection."
        connection.close()
