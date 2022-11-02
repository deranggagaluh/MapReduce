from dataclasses import replace
from msilib import schema
import pandas as pd
import sqlalchemy

#Read source csv file
data_property = pd.read_csv('F:\Docker\hadoop\dataset\TR_PropertyInfo.csv')
data_userinfor = pd.read_csv('F:\Docker\hadoop\dataset\TR_UserInfo.csv')
data_product = pd.read_csv('F:\Docker\hadoop\dataset\TR_Products.csv')
data_orderdetail = pd.read_csv('F:\Docker\hadoop\dataset\TR_OrderDetails.csv')

#init var col table
col_property = {"Prop ID":"Prop_ID","PropertyCity":"Property_city","PropertyState":"Property_State"}
col_userinfo = {"UserID":"User_ID","UserSex":"User_Sex","UserDevice":"User Device"}
col_product = {"ProductID":"Product_id","ProductName":"Product_name","ProductCategory":"Product_category","Price":"Price"}
col_orderdetail = {"OrderID":"Order_id","OrderDate":"Oder_date","PropertyID":"Property_id","ProductID":"Product_id","Quantity":"Quantity"}

#rename col table
data_property = data_property.rename(columns=col_property)
data_userinfo = data_userinfor.rename(columns=col_userinfo)
data_product = data_product.rename(columns=col_product)
data_orderdetail = data_orderdetail.rename(columns=col_orderdetail)

#create postgres 
Conn = sqlalchemy.create_engine(url='postgresql://degaraja:123456789@localhost:5432/postgres')

#write to database postgres
data_property.to_sql("property",con=Conn, index=False, if_exists='replace')
data_userinfo.to_sql("userinfo", con=Conn, index=False, if_exists='replace')
data_product.to_sql("product",con=Conn, index=False, if_exists='replace')
data_orderdetail.to_sql("orderdetail",con=Conn, index=False, if_exists='replace')
