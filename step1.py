import os
import psycopg2
from datetime import date

file = open("step1.log", "w")
file.write("step1attempt")
file.close()

connection = psycopg2.connect("dbname=northwind user=northwind_user host=localhost port=5441 password=thewindisblowing")
cursor = connection.cursor()

cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
tables = cursor.fetchall()

date = str(date.today())

#Extracting data from source database:
for table in tables:
    table = str(table).replace("('", "").replace("',)","")
    print(table)

    filename = "data/postgres/" + table + "/" + date + "/file.csv"
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    file = open(filename, "w")

    sql_command = "COPY (SELECT * FROM " + table + ") TO stdout WITH CSV HEADER DELIMITER ';'"
    cursor.copy_expert(sql_command, file)

file.close()
cursor.close()
connection.close()

#Extracting data from source CSV file (changing the separator to ;):
print("order_details (from CSV)")
file_read = open("data/order_details.csv", "r")

filename = "data/csv/" + date + "/file.csv"
os.makedirs(os.path.dirname(filename), exist_ok=True)
file_write = open(filename, "w")

while True:
    row = file_read.readline()
    if (row == ""):
        break
    file_write.write(str(row).replace(",", ";"))

file_read.close()
file_write.close()

print("Extracted data from different sources to local filesystem")
file = open("step1.log", "w")
file.write("step1success")
file.close()
