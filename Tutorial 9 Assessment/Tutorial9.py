import mysql.connector
from mysql.connector import Error

try:
    # Connect to MySQL
    connection = mysql.connector.connect(
        host='localhost',
        database='world',
        user='root',
        password=''
    )
    
    if connection.is_connected():
        cursor = connection.cursor()

        # Begin Transaction
        connection.start_transaction()

        # Insert a new country
        cursor.execute("""
        INSERT INTO Country (Code, Name, Continent, Region, SurfaceArea, Population, LocalName, GovernmentForm)
        VALUES ('XYZ', 'NewCountry', 'Asia', 'NewRegion', 500000.00, 1000000, 'LocalNewCountry', 'Democracy');
        """)
        
        # Insert a new language for the country
        cursor.execute("""
        INSERT INTO CountryLanguage (CountryCode, Language, IsOfficial, Percentage)
        VALUES ('XYZ', 'NewLanguage', 'T', 90.0);
        """)
        
        # Attempt to insert a new city with an invalid CountryCode (too long)
        cursor.execute("""
        INSERT INTO City (Name, CountryCode, District, Population)
        VALUES ('NewCapitalCity', 'XYZ_INVALID', 'NewDistrict', 500000);
        """)
        
        # This line won't be reached due to the error above
        connection.commit()
        print("Transaction committed successfully!")

except Error as e:
    # Rollback the transaction in case of failure
    print(f"Error occurred: {e}")
    connection.rollback()
    print("Transaction rolled back due to error.")

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection closed")