import os
import sys
import psycopg2
from datetime import date

file = open("step3.log", "w")
file.write("step3attempt")
file.close()

try:
    file = open("step2.log")
    info = file.readline()
    print(info)
    if info != "step2success":
        raise Exception()
except:
    print("It looks like the step2.py did not run successfully. You must run step2.py first! Bye")
    sys.exit()

try:
    date = str(sys.argv[1])
    print("Attempt to work with the date: " + date)
except:
    date = str(date.today())
    print("Attempt to work with today date")

connection = psycopg2.connect("dbname=final_db host=localhost port=5442 user=postgres password=postgres")
print("Connected to the database")
cursor = connection.cursor()

try:
    #Adding data extracted from souce database to final database
    dir_list = os.listdir("data/postgres")
    for table_name in dir_list:
        filename = "data/postgres/" + table_name + "/" + date + "/file.csv"
        file = open(filename, 'r')
        sql_command = "COPY " + table_name + " FROM stdin WITH CSV HEADER DELIMITER ';' NULL ''"
        cursor.copy_expert(sql_command, file)
        print(table_name)

    #Adding order_details extracted from source CSV file to final database
    print('order_details')
    filename = "data/csv/" + date + "/file.csv"
    file = open(filename, 'r')

    sql_command = "COPY order_details FROM stdin WITH CSV HEADER DELIMITER ';' NULL ''"
    cursor.copy_expert(sql_command, file)
    print("Inserted data on the final database")

    connection.commit()

    file = open("step3.log", "w")
    file.write("step3success\n")
    file.write(date)
    file.close()
except Exception as exception:
    print("It looks like there is no data extracted on selected day or there is an error on your command typing.")
    print("You can extract data from different sources running step1.py in different days. Bye")
    print(exception)
finally:
    cursor.close()
    connection.close()
