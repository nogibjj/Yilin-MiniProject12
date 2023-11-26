import mlflow
mlflow.set_tracking_uri("http://127.0.0.1:5000")

"""
ETL-Query script
"""

# Code from extract.py
"""
Extract a dataset from a URL like Kaggle or data.gov. 
JSON or CSV formats tend to work well

food dataset
"""
import requests

def extract(url="https://raw.githubusercontent.com/"
                "Barabasi-Lab/GroceryDB/main/data/GroceryDB_IgFPro.csv",
            file_path="data/GroceryDB_IgFPro.csv"):
    """"Extract a url to a file path"""
    with requests.get(url) as r:
        with open(file_path, 'wb') as f:
            f.write(r.content)
    return file_path




# Code from transform_load.py
"""
Transforms and Loads data into the local SQLite3 database
Example:
,general name,count_products,ingred_FPro,avg_FPro_products,
avg_distance_root,ingred_normalization_term,semantic_tree_name,semantic_tree_node
"""
import sqlite3
import csv
import os

#load the csv file and insert into a new sqlite3 database
def load(dataset="data/GroceryDB_IgFPro.csv"):
    """"Transforms and Loads data into the local SQLite3 database"""

    #prints the full working directory and path
    print(os.getcwd())
    payload = csv.reader(open(dataset, newline=''), delimiter=',')
    conn = sqlite3.connect('GroceryDB.db')
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS GroceryDB")
    c.execute("CREATE TABLE GroceryDB (id,general_name, count_products, ingred_FPro,"
               "avg_FPro_products, avg_distance_root, ingred_normalization_term,"
                " semantic_tree_name, semantic_tree_node)")
    #insert
    c.executemany("INSERT INTO GroceryDB VALUES (?,?, ?, ?, ?, ?, ?, ?, ?)", payload)
    conn.commit()
    conn.close()
    return "GroceryDB.db"


# Code from query.py
"""Query the database"""

import sqlite3

from prettytable import PrettyTable

def query1():
    """Query the database for the top 5 rows of the GroceryDB table"""
    conn = sqlite3.connect("GroceryDB.db")
    cursor = conn.cursor()
    # Skipping the first row (header)
    cursor.execute("SELECT * FROM GroceryDB LIMIT 5 OFFSET 1") 
    
    # Fetching column names
    column_names = [description[0] for description in cursor.description]
    
    table = PrettyTable(column_names)  # Initializing table with column names

    # Fetching rows and adding them to the table
    for row in cursor.fetchall():
        table.add_row(row)
    
    print("Top 5 rows of the GroceryDB:")
    print(table)  # Printing table
    
    conn.close()
    return "Success"

def query2():
    '''Update the count_products of the arabica coffee in the GroceryDB table'''
    conn = sqlite3.connect("GroceryDB.db")
    cursor = conn.cursor()

    # Define the new count_products value
    new_count_products = 100
    item_name = "arabica coffee"

    cursor.execute("UPDATE GroceryDB SET count_products = ? WHERE general_name = ?",
                    (new_count_products, item_name))
    conn.commit()
    conn.close()
    # Print the updated row
    print("Updated row:")
    query1()
    return "Update Success"

def query3():
    '''INSERT a new row into the GroceryDB table'''
    conn = sqlite3.connect("GroceryDB.db")
    cursor = conn.cursor()

    # Define the values for the new row
    values = ('new_general_name', 10, 0.1, 0.2, 3.0, 10.0,
               'new_tree_name', 'new_tree_node')

    cursor.execute("""
        INSERT INTO GroceryDB (general_name, count_products, ingred_FPro, 
        avg_FPro_products, avg_distance_root, ingred_normalization_term, 
        semantic_tree_name, semantic_tree_node) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, values) 
    
    conn.commit()
    
    # Retrieve and print the inserted row based on a unique field, e.g., general_name
    cursor.execute("SELECT * FROM GroceryDB WHERE general_name = ?", (values[0],))
    row = cursor.fetchone()
    
    if row:
        column_names = [description[0] for description in cursor.description]
        table = PrettyTable(column_names)  # Initializing table with column names
        table.add_row(row)
        print("Inserted new row:")
        print(table)
    else:
        print("Inserted row could not be retrieved")
        
    conn.close()
    return "Insert Success"

def query4():
    '''DELETE the row containing arabica coffee in the GroceryDB table'''
    conn = sqlite3.connect("GroceryDB.db")
    cursor = conn.cursor()

    # Define the item_name to delete
    item_name = "arabica coffee"

    # Execute DELETE command
    cursor.execute("DELETE FROM GroceryDB WHERE general_name = ?", (item_name,))
    conn.commit()

    # Print the updated rows
    print("Deleted rows containing arabica coffee:")
    query1()

    conn.close()
    return "Delete Success"





# Modified main.py content
"""
ETL-Query script
"""





mlflow.start_run()

# Extract
print("Extracting data...")
extract()

# Transform and load
print("Transforming data...")
load()

# Query
print("Querying data...")
query1()
print()
query2()
print()
query3()
print()
query4()
# Example MLflow tracking
# Tracking the URL used in the extract function
mlflow.log_param("data_url", "https://raw.githubusercontent.com/Barabasi-Lab/GroceryDB/main/data/GroceryDB_IgFPro.csv")

# Tracking a metric, for example, a dummy metric
mlflow.log_metric("dummy_metric", 1.0)

# Logging an artifact, here the output file from the load function
mlflow.log_artifact("data/GroceryDB_IgFPro.csv")

# Ending the MLflow run
mlflow.end_run()
