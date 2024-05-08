import mysql.connector
import pandas as pd
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Epcot2024!",
    database="mydatabase"
)
mycursor = mydb.cursor()

query = "INSERT INTO airports (iata_code, airport_name) VALUES (%s, %s)"
airports = pd.read_csv("airports.csv", keep_default_na=False)


for index, row in airports.iterrows():
    print(row["Description"])
    airport_name = row["Description"].split(": ")
    if(len(airport_name) > 1):
        desc = airport_name[1]
    else:
        desc = row["Description"]
    val = (row["Code"], desc)
    mycursor.execute(query, val)
    mydb.commit()
    print(mycursor.rowcount, "record inserted.")
