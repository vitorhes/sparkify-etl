import psycopg2
from sql_queries import *
from etl import etl_process

def create_database_connection ():
    '''
    Delete database 'sparkify' and then re-create it
    Set connection and return cursor
    '''
    conn = psycopg2.connect("user=postgres password=postgres")
    cur = conn.cursor()
    conn.set_session(autocommit = True)

    #drop/create database
    cur.execute("DROP DATABASE IF EXISTS sparkify")
    try:
        cur.execute("CREATE DATABASE sparkify")
    except:
        print("Database already exist")

    print("DataBase created")


    conn = psycopg2.connect("dbname=sparkify user=postgres password=postgres")
    cur = conn.cursor()
    conn.set_session(autocommit = True)
    print("Sucessfuly connected to the databse")

    return cur,conn

def create_tables(cur):

    for table in table_list:
        cur.execute(table)
        print("Table created")

def drop_tables(cur):

    '''
    Drop tables if exist
    '''

    for table in drop_table_list:
        cur.execute(table)
        print("Table dropped")

def print_schema(cur):      
    '''
    Print the schema of the tables created. To make sure it`s correct
    '''

    print("----------------SCHEMA CREATED-----------------")
    for table in tables_names_list:
        sql_print_table = "SELECT * FROM "+table+" LIMIT 0"
        cur.execute(sql_print_table)
        colnames = [desc[0] for desc in cur.description]
        
        print(colnames)


def tester (cur):
    '''
    Query data from a table an print it, to make sure it was loaded properly
    '''

    print("-----Testing: querying data from 'artists' table-----")
    cur.execute("SELECT * FROM artists")

    row = cur.fetchone()
    while row:
        print(row)
        row = cur.fetchmany()
    print("Done testing")


def main():
    '''
    Delete and create database `sparkify`
    Set connection to database
    Drop all tables and re-create them
    Initialize the ETL process
    Close the connection   
    
    '''
    cur,conn = create_database_connection()
    
    drop_tables(cur)

    create_tables(cur)

    print_schema(cur)

    '''
    Initialize ETL process, query some data, and the close connection
    '''
    etl_process(cur)
    
    tester(cur)
    conn.close()
    print("---Connection Closed. ETL process is done--")
    
if __name__ == "__main__":
    main()