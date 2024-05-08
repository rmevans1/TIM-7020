import mysql.connector
import pandas as pd
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Epcot2024!",
    database="mydatabase"
)
mycursor = mydb.cursor()

query = "INSERT INTO states (fips_code, state_name) VALUES (%s, %s)"
states = pd.read_csv("state_fips.csv", keep_default_na=False)

for index, row in states.iterrows():
    val = (row["Code"], row["Description"])
    mycursor.execute(query, val)
    mydb.commit()
    print(mycursor.rowcount, "record inserted.")
