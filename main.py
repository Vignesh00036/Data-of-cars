import pandas as pd
import mysql.connector
import boto3
import psycopg2

#Establishing the connection
s3=boto3.client('s3')
data = pd.read_csv('{path_to_your_data_file}')
conn = mysql.connector.connect(database='database_name', user='user_name', password='user_password')
mysql_cursor=conn.cursor()
redshift_conn = psycopg2.connect(host='redshift_endpoint', database='database_name', user='user_name', password='user_password', port='port_number')
redshift_cursor=redshift_conn.cursor()

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
       CREATE TABLE IF NOT EXISTS cars (S_No int primary key, Name varchar(100), Location varchar(50), Year int, Kilometers_Driven int,
       Fuel_Type varchar(50), Transmission varchar(50), Owner_Type varchar(50), Mileage float, Engine int, Power float, 
       Seats float, New_Price int, Price int);
'''
mysql_cursor.execute(create_table_statement)
redshift_cursor.execute(create_table_statement)
redshift_conn.commit()
conn.commit()

#Insert statement for inserting a values
insert_statement='''
        insert IGNORE into cars values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
'''

#Converting values into tuple and inserting into table
for record in [tuple(x) for x in data.to_numpy()]:
    mysql_cursor.execute(insert_statement, record)
    conn.commit()

#Exporting the data
data.to_csv('path_to_your_output_destination', index=False)

#Uploading to s3
file_path='path_to_your_output_file'
s3_path='data.csv'
s3.upload_file(file_path, 'bucket_name', s3_path)

#loading data into redshift table
copy_statement=""" 
    COPY dev.public.cars
    FROM 'path_to_your_file' 
    IAM_ROLE 'iam_role_arn' 
    FORMAT AS CSV 
    DELIMITER ',' 
    QUOTE '"' 
    IGNOREHEADER 1 
    REGION AS 'region';
 """

#saving the changes
redshift_conn.commit()
