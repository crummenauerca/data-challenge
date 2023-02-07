# code-challenge solution v1 (using only Docker, Python and psycopg2 lib)
Simple solution to resolve the challenge (using only Docker, Python and psycopg2 lib)

- Note 1: The extracted data will be organized inside the folder named as "data"
- Note 2: The simple log files are used to help running thes scripts in a valid sequence
- Note 3: For simplicity, login credentials are hard-coded in scripts and versioned by Git/GitHub

# Video about this solution:
https://youtu.be/aZYsdALuyj8

# How to prepare the simple environment:
- Download and install Docker from: https://www.docker.com/. A reboot may be required
- Prepare the source database and the final database (ports mapped to 5441 and 5442) running:
```
docker-compose up
```
- Download and install Python (with pip and terminal integration) from: https://www.python.org/. A reboot may be required
- Install psycopg2 (lib to connect with PostgreSQL) running:
```
pip install psycopg2
```

# How to run:
- To extract the data from the different sources to local file system (on the data folder), run:
```
python step1.py
```

- To prepare the final database (creating or recreating tables), run:
```
python step2.py
```

- To insert the extracted data to the final database, run:
```
python step3.py
```
(add an argument (AAAA-MM-DD) if you want to use extracted data in a past day. For example run: 
```
python step3.py 2023-02-07
```

- To add referential integrity constraints to the final database, run:
```
python step4.py
```

- To get a sample of related data (products, order_detais, orders, customers) from the final database into a CSV file (data/result/{date}_data_sample.csv) run:
```
python step5.py
```

Please contact-me to know more :)