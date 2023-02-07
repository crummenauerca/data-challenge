import sys
import psycopg2

file = open("step4.log", "w")
file.write("step4attempt")
file.close()

try:
    file = open("step2.log")
    info = file.readline()
    if info != "step2success":
        raise Exception()
except:
    print("It looks like the step2.py did not run successfully. You must run step2.py to the create data structure first! Bye")
    sys.exit()

try:
    file = open("step3.log")
    info = file.readline()
    if info != "step3success\n":
        raise Exception()
except:
    print("It looks like the step3.py did not run successfully. You should run step3.py first")
    print("Creating referential integrity constraints can difficult freely insert data")
    print("You can recreate the data structure running the step2.py again if you need it. Bye")
    sys.exit()

connection = psycopg2.connect("dbname=final_db host=localhost port=5442 user=postgres password=postgres")
print("Connected to the database")
cursor = connection.cursor()

sql_commands = """
ALTER TABLE ONLY categories
    ADD CONSTRAINT pk_categories PRIMARY KEY (category_id);

ALTER TABLE ONLY customer_customer_demo
    ADD CONSTRAINT pk_customer_customer_demo PRIMARY KEY (customer_id, customer_type_id);

ALTER TABLE ONLY customer_demographics
    ADD CONSTRAINT pk_customer_demographics PRIMARY KEY (customer_type_id);

ALTER TABLE ONLY customers
    ADD CONSTRAINT pk_customers PRIMARY KEY (customer_id);

ALTER TABLE ONLY employees
    ADD CONSTRAINT pk_employees PRIMARY KEY (employee_id);

ALTER TABLE ONLY employee_territories
    ADD CONSTRAINT pk_employee_territories PRIMARY KEY (employee_id, territory_id);

ALTER TABLE ONLY orders
    ADD CONSTRAINT pk_orders PRIMARY KEY (order_id);

ALTER TABLE ONLY products
    ADD CONSTRAINT pk_products PRIMARY KEY (product_id);

ALTER TABLE ONLY region
    ADD CONSTRAINT pk_region PRIMARY KEY (region_id);

ALTER TABLE ONLY shippers
    ADD CONSTRAINT pk_shippers PRIMARY KEY (shipper_id);

ALTER TABLE ONLY suppliers
    ADD CONSTRAINT pk_suppliers PRIMARY KEY (supplier_id);

ALTER TABLE ONLY territories
    ADD CONSTRAINT pk_territories PRIMARY KEY (territory_id);

ALTER TABLE ONLY us_states
    ADD CONSTRAINT pk_usstates PRIMARY KEY (state_id);

ALTER TABLE ONLY orders
    ADD CONSTRAINT fk_orders_customers FOREIGN KEY (customer_id) REFERENCES customers;

ALTER TABLE ONLY orders
    ADD CONSTRAINT fk_orders_employees FOREIGN KEY (employee_id) REFERENCES employees;

ALTER TABLE ONLY orders
    ADD CONSTRAINT fk_orders_shippers FOREIGN KEY (ship_via) REFERENCES shippers;

ALTER TABLE ONLY products
    ADD CONSTRAINT fk_products_categories FOREIGN KEY (category_id) REFERENCES categories;

ALTER TABLE ONLY products
    ADD CONSTRAINT fk_products_suppliers FOREIGN KEY (supplier_id) REFERENCES suppliers;

ALTER TABLE ONLY territories
    ADD CONSTRAINT fk_territories_region FOREIGN KEY (region_id) REFERENCES region;

ALTER TABLE ONLY employee_territories
    ADD CONSTRAINT fk_employee_territories_territories FOREIGN KEY (territory_id) REFERENCES territories;

ALTER TABLE ONLY employee_territories
    ADD CONSTRAINT fk_employee_territories_employees FOREIGN KEY (employee_id) REFERENCES employees;

ALTER TABLE ONLY customer_customer_demo
    ADD CONSTRAINT fk_customer_customer_demo_customer_demographics FOREIGN KEY (customer_type_id) REFERENCES customer_demographics;

ALTER TABLE ONLY customer_customer_demo
    ADD CONSTRAINT fk_customer_customer_demo_customers FOREIGN KEY (customer_id) REFERENCES customers;

ALTER TABLE ONLY employees
    ADD CONSTRAINT fk_employees_employees FOREIGN KEY (reports_to) REFERENCES employees;

ALTER TABLE ONLY order_details
    ADD CONSTRAINT fk_order_details_orders FOREIGN KEY (order_id) REFERENCES orders;

ALTER TABLE ONLY order_details
    ADD CONSTRAINT fk_order_details_products FOREIGN KEY (product_id) REFERENCES products;
"""

cursor.execute(sql_commands)
connection.commit()
print("Created referential integrity constraints")
file = open("step4.log", "w")
file.write("step4success")
file.close()
cursor.close()
connection.close()
