import psycopg2

from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()
# get environment variables
dbname, host, user, password, port = os.getenv('DBNAME'), os.getenv('HOST'), os.getenv('DBUSER'), os.getenv('PASS'), os.getenv('PORT')

conn = psycopg2.connect(database=dbname,
                        host=host,
                        user=user,
                        password=password,
                        port=port)

cursor = conn.cursor()
# use the cursor to interact with your database
cursor.execute("SELECT 'hello world'")
print(cursor.fetchone())

cursor = conn.cursor()
query = "SELECT * FROM sensor_data;"
cursor.execute(query)
for row in cursor.fetchall():
    print(row)
cursor.close()

# query with placeholders
cursor = conn.cursor()
query = """
           SELECT time_bucket('5 minutes', time) AS five_min, avg(cpu)
           FROM sensor_data
           JOIN sensors ON sensors.id = sensor_data.sensor_id
           WHERE sensors.location = %s AND sensors.type = %s
           GROUP BY five_min
           ORDER BY five_min DESC;
           """
location = "floor"
sensor_type = "a"
data = (location, sensor_type)
cursor.execute(query, data)
results = cursor.fetchall()