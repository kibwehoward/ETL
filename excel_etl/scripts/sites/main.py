import psycopg2
from db_config import db_credentials


def get_sites_list():
    # Database connection parameters
    u, p, h, port, db = db_credentials.values()

    try:
        # Establishing the connection
        conn = psycopg2.connect(
            dbname=db, user=u, password=p, host=h, port=port
        )

        cur = conn.cursor()

        # Executing a query to fetch the "SITES" column
        cur.execute('SELECT "SITES" FROM excel_etl')

        # Fetching all rows from the table
        rows = cur.fetchall()

        # Closing the connection
        cur.close()
        conn.close()

        # Extracting and alphabetically sorting the values from the "SITES" column
        sites_set = set()  # Use a set to store unique values

        for row in rows:
            value = row[0]
            if value is not None:
                sites_set.update(value.split(','))  # Add values to the set

        sites_list = sorted(sites_set)  # Sort the unique values and convert to a list

        return sites_list
    except psycopg2.Error as e:
        print(f"Error: {e}")
        return None


def main():
    sites_list = get_sites_list()

    if sites_list is not None:
        for site in sites_list:
            print(site)


if __name__ == "__main__":
    main()
