import mysql.connector
import pandas as pd
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Epcot2024!",
    database="mydatabase"
)
mycursor = mydb.cursor()

query = "INSERT INTO airlines (iata_code, airline_name) VALUES (%s, %s)"
airports = pd.read_csv("carriers.csv", keep_default_na=False)


for index, row in airports.iterrows():
    val = (row["Code"], row["Description"])
    mycursor.execute(query, val)
    mydb.commit()
    print(mycursor.rowcount, "record inserted.")
