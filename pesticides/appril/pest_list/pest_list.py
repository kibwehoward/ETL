import psycopg2
from db_config import db_credentials

def get_pests_list():
    # Database connection parameters
    u, p, h, port, db = db_credentials.values()

    # Establishing the connection
    conn = psycopg2.connect(
        dbname=db, user=u, password=p, host=h, port=port
    )
    cur = conn.cursor()

    # Executing a query
    cur.execute('SELECT DISTINCT "PESTS" FROM appril')

    # Fetching all rows from the table
    pests = cur.fetchall()

    # Closing the connection
    cur.close()
    conn.close()

    return pests

def main():
    pests_list = get_pests_list()
    for pest in pests_list:
        print(pest[0])

if __name__ == "__main__":
    main()