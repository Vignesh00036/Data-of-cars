import pandas as pd
import mysql.connector
import boto3

#Establishing the connection
s3=boto3.client('s3')
data = pd.read_csv(r'/home/beast/Downloads/used_cars_data.csv')
conn = mysql.connector.connect(database='data', user='root', password='@Beast00036@')
cursor=conn.cursor()

#Data cleansing
data['Power']=data['Power'].str.replace(' bhp', '', regex=False)
data['Power']=data['Power'].replace('null',0)
data['Mileage']=data['Mileage'].str.replace(' kmpl', '', regex=False)
data['Mileage']=data['Mileage'].str.replace(' km/kg', '', regex=False)
data['Engine']=data['Engine'].str.replace(' CC', '', regex=False)
new_price=[]
price=[]

#Converting price from string to numbers(e.x 25 lakh to 2500000) for column New_price
for i in data['New_Price']:
    if ' lakh' in str(i).lower():
        lakh=str(i).lower().replace(' lakh','')
        if '.' in lakh:
            thousand=lakh.split('.')
            new_price.append(int(thousand[0])*100000+int(thousand[1])*1000)
        else:
            new_price.append(int(lakh)*100000)
    else:
        new_price.append(0)

#Converting price from string to numbers(e.x 25 lakh to 2500000) for column Price
for j in data['Price'].fillna(0):
    if '.' in str(j):
        thousands=str(j).split('.')
        price.append(int(thousands[0])*100000+int(thousands[1])*1000)
    else:
        price.append(int(j)*100000)

#Updating variables
data['New_Price']=new_price
data['Price']=price


#Creating table in MySql
create_table_statement='''
       CREATE TABLE IF NOT EXISTS cars (S_No int primary key, Name varchar(50), Location varchar(50), Year int, Kilometers_Driven int,
       Fuel_Type varchar(50), Transmission varchar(50), Owner_Type varchar(50), Mileage float, Engine int, Power float, 
       Seats int, New_Price int, Price int);
'''

#Insert statement for inserting a values
insert_statement='''
        insert IGNORE into cars values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
'''
cursor.execute(create_table_statement)
conn.commit()

#Converting values into tuple and inserting into table
for record in [tuple(x) for x in data.to_numpy()]:
    cursor.execute(insert_statement, record)
    conn.commit()

#Exporting the data
data.to_csv('/media/beast/Beast/DE/Python_programms/2_OG/data/data.csv', index=False)

#Uploading to s3
file_path='/media/beast/Beast/DE/Python_programms/2_OG/data/data.csv'
s3_path='data.csv'
s3.upload_file(file_path, 'storage-for-mysql', s3_path)