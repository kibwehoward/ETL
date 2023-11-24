import psycopg2
from db_config import db_credentials


def get_pests_list():
    # Database connection parameters
    u, p, h, port, db = db_credentials.values()

    try:
        # Establishing the connection
        conn = psycopg2.connect(
            dbname=db, user=u, password=p, host=h, port=port
        )

        cur = conn.cursor()

        # Executing a query to fetch the "PESTS" column
        cur.execute('SELECT "PESTS" FROM appril')

        # Fetching all rows from the table
        rows = cur.fetchall()

        # Closing the connection
        cur.close()
        conn.close()

        # Extracting and alphabetically sorting the values from the "PESTS" column
        pests_set = set()  # Use a set to store unique values

        for row in rows:
            value = row[0]
            if value is not None:
                pests_set.update(value.split(','))  # Add values to the set

        pests_list = sorted(pests_set)  # Sort the unique values

        return pests_list
    except psycopg2.Error as e:
        print(f"Error: {e}")
        return None


def main():
    pests_list = get_pests_list()

    if pests_list is not None:
        for pest in pests_list:
            print(pest)


if __name__ == "__main__":
    main()
