import os
import sys
import psycopg2

file = open("step5.log", "w")
file.write("step5attempt")
file.close()

try:
    file = open("step3.log")
    info = file.readline()
    if info != "step3success\n":
        raise Exception()
except:
    print("It looks like the step3.py did not run successfully. You must run step3.py first :)")
    print("You need to load the final database before generating a CSV file of the final database data. Bye")
    sys.exit()

date = str(file.readline()) # get date used to load the final database in the step3.py script

connection = psycopg2.connect("dbname=final_db host=localhost port=5442 user=postgres password=postgres")
connection.set_client_encoding('UTF-8')
print("Connected to the database")
cursor = connection.cursor()

sql_command = """
COPY (SELECT * FROM products, order_details, orders, customers
WHERE products.product_id = order_details.product_id
AND order_details.order_id = orders.order_id
AND customers.customer_id = orders.customer_id) TO STDOUT WITH CSV DELIMITER ';'
"""

filename = "data/result/" + date + "_data_sample.csv"
os.makedirs(os.path.dirname(filename), exist_ok=True)
file = open(filename, "w")
cursor.copy_expert(sql_command, file)
print("Created a CSV from final database (that was loaded with data from different sources)")

file = open("step5.log", "w")
file.write("step5success")
file.close()
cursor.close()
connection.close()
