import os
import psycopg2
from datetime import date

today = str(date.today())

def extracts_data_from_source_database():
    connection = psycopg2.connect("dbname=northwind user=northwind_user host=host.docker.internal port=5441 password=thewindisblowing")
    cursor = connection.cursor()

    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = cursor.fetchall()

    for table in tables:
        table = str(table).replace("('", "").replace("',)","")

        filename = "data/postgres/" + table + "/" + today + "/file.csv"
        os.makedirs(os.path.dirname(filename), exist_ok = True)
        file = open(filename, "w")

        sql_command = "COPY (SELECT * FROM " + table + ") TO stdout WITH CSV HEADER DELIMITER ';'"
        cursor.copy_expert(sql_command, file)
    
    file.close()
    cursor.close()
    connection.close()

def extracts_data_from_source_csv_file():
    file_read = open("data/order_details.csv", "r")

    filename = "data/csv/" + today + "/file.csv"
    os.makedirs(os.path.dirname(filename), exist_ok = True)
    file_write = open(filename, "w")

    while True:
        row = file_read.readline()
        if (row == ""):
            break
        file_write.write(str(row).replace(",", ";"))

    file_read.close()
    file_write.close()

def creates_final_database_structures():
    connection = psycopg2.connect("dbname=final_db host=host.docker.internal port=5442 user=postgres password=postgres")
    cursor = connection.cursor()

    sql_commands = """
        SET statement_timeout = 0;
        SET lock_timeout = 0;
        SET client_encoding = 'UTF8';
        SET standard_conforming_strings = on;
        SET check_function_bodies = false;
        SET client_min_messages = warning;

        SET default_tablespace = '';
        SET default_with_oids = false;

        DROP TABLE IF EXISTS customer_customer_demo CASCADE;
        DROP TABLE IF EXISTS customer_demographics CASCADE;
        DROP TABLE IF EXISTS employee_territories CASCADE;
        DROP TABLE IF EXISTS orders CASCADE;
        DROP TABLE IF EXISTS customers CASCADE;
        DROP TABLE IF EXISTS products CASCADE;
        DROP TABLE IF EXISTS shippers CASCADE;
        DROP TABLE IF EXISTS suppliers CASCADE;
        DROP TABLE IF EXISTS territories CASCADE;
        DROP TABLE IF EXISTS us_states CASCADE;
        DROP TABLE IF EXISTS categories CASCADE;
        DROP TABLE IF EXISTS region CASCADE;
        DROP TABLE IF EXISTS employees CASCADE;

        CREATE TABLE categories (
            category_id smallint NOT NULL,
            category_name character varying(15) NOT NULL,
            description text,
            picture bytea
        );

        CREATE TABLE customer_customer_demo (
            customer_id bpchar NOT NULL,
            customer_type_id bpchar NOT NULL
        );

        CREATE TABLE customer_demographics (
            customer_type_id bpchar NOT NULL,
            customer_desc text
        );

        CREATE TABLE customers (
            customer_id bpchar NOT NULL,
            company_name character varying(40) NOT NULL,
            contact_name character varying(30),
            contact_title character varying(30),
            address character varying(60),
            city character varying(15),
            region character varying(15),
            postal_code character varying(10),
            country character varying(15),
            phone character varying(24),
            fax character varying(24)
        );

        CREATE TABLE employees (
            employee_id smallint NOT NULL,
            last_name character varying(20) NOT NULL,
            first_name character varying(10) NOT NULL,
            title character varying(30),
            title_of_courtesy character varying(25),
            birth_date date,
            hire_date date,
            address character varying(60),
            city character varying(15),
            region character varying(15),
            postal_code character varying(10),
            country character varying(15),
            home_phone character varying(24),
            extension character varying(4),
            photo bytea,
            notes text,
            reports_to smallint,
            photo_path character varying(255)
        );

        CREATE TABLE employee_territories (
            employee_id smallint NOT NULL,
            territory_id character varying(20) NOT NULL
        );

        CREATE TABLE orders (
            order_id smallint NOT NULL,
            customer_id bpchar,
            employee_id smallint,
            order_date date,
            required_date date,
            shipped_date date,
            ship_via smallint,
            freight real,
            ship_name character varying(40),
            ship_address character varying(60),
            ship_city character varying(15),
            ship_region character varying(15),
            ship_postal_code character varying(10),
            ship_country character varying(15)
        );

        CREATE TABLE products (
            product_id smallint NOT NULL,
            product_name character varying(40) NOT NULL,
            supplier_id smallint,
            category_id smallint,
            quantity_per_unit character varying(20),
            unit_price real,
            units_in_stock smallint,
            units_on_order smallint,
            reorder_level smallint,
            discontinued integer NOT NULL
        );

        CREATE TABLE region (
            region_id smallint NOT NULL,
            region_description bpchar NOT NULL
        );

        CREATE TABLE shippers (
            shipper_id smallint NOT NULL,
            company_name character varying(40) NOT NULL,
            phone character varying(24)
        );

        CREATE TABLE suppliers (
            supplier_id smallint NOT NULL,
            company_name character varying(40) NOT NULL,
            contact_name character varying(30),
            contact_title character varying(30),
            address character varying(60),
            city character varying(15),
            region character varying(15),
            postal_code character varying(10),
            country character varying(15),
            phone character varying(24),
            fax character varying(24),
            homepage text
        );

        CREATE TABLE territories (
            territory_id character varying(20) NOT NULL,
            territory_description bpchar NOT NULL,
            region_id smallint NOT NULL
        );

        CREATE TABLE us_states (
            state_id smallint NOT NULL,
            state_name character varying(100),
            state_abbr character varying(2),
            state_region character varying(50)
        );

        DROP TABLE IF EXISTS order_details;

        CREATE TABLE order_details (
            order_id int,
            product_id smallint,
            unit_price real,
            quantity smallint,
            discount real
        )
    """
    
    cursor.execute(sql_commands)
    
    connection.commit()
    cursor.close()
    connection.close()

def inserts_extracted_data_from_source_database_into_final_database():
    connection = psycopg2.connect("dbname=final_db host=host.docker.internal port=5442 user=postgres password=postgres")
    cursor = connection.cursor()

    dir_list = os.listdir("data/postgres")
    for table_name in dir_list:
        filename = "data/postgres/" + table_name + "/" + today + "/file.csv"
        file = open(filename, 'r')
        sql_command = "COPY " + table_name + " FROM stdin WITH CSV HEADER DELIMITER ';' NULL ''"
        cursor.copy_expert(sql_command, file)    

    connection.commit()
    cursor.close()
    connection.close()

def inserts_extracted_data_from_source_csv_file_into_final_database():
    connection = psycopg2.connect("dbname=final_db host=host.docker.internal port=5442 user=postgres password=postgres")
    cursor = connection.cursor()

    filename = "data/csv/" + today + "/file.csv"
    file = open(filename, 'r')

    sql_command = "COPY order_details FROM stdin WITH CSV HEADER DELIMITER ';' NULL ''"
    cursor.copy_expert(sql_command, file)

    connection.commit()
    cursor.close()
    connection.close()

def creates_referential_integrity_constraints_on_the_final_database():
    connection = psycopg2.connect("dbname=final_db host=host.docker.internal port=5442 user=postgres password=postgres")
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
    cursor.close()
    connection.close()

def gets_sample_of_relationated_data_from_final_database():
    connection = psycopg2.connect("dbname=final_db host=host.docker.internal port=5442 user=postgres password=postgres")
    cursor = connection.cursor()

    sql_command = """
        COPY (SELECT * FROM products, order_details, orders, customers
        WHERE products.product_id = order_details.product_id
        AND order_details.order_id = orders.order_id
        AND customers.customer_id = orders.customer_id) TO STDOUT WITH CSV DELIMITER ';'
    """

    filename = "data/" + today + "_final_data_sample.csv"
    os.makedirs(os.path.dirname(filename), exist_ok = True)
    file = open(filename, "w")
    cursor.copy_expert(sql_command, file)

    file.close()
    cursor.close()
    connection.close()