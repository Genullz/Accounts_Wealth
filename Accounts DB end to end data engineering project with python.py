#!/usr/bin/env python
# coding: utf-8

# #### Initializing our Packages

# In[1]:


get_ipython().system('pip install psycopg2')


# In[2]:


import psycopg2
import pandas as pd


# #### Creating our functions

# In[3]:


def create_database():
	#connects to default database
	#conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=root")
	conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=Genullz86")
	conn.set_session(autocommit=True)
	cur = conn.cursor()

	# create sparkify database with UTF8 ending
	cur.execute("DROP DATABASE accounts")
	cur.execute("CREATE DATABASE accounts")

	# close connection to default database
	conn.close()

	# connect to sparkify database
	conn = psycopg2.connect("host=127.0.0.1 dbname=accounts user=postgres password=Genullz86")
	cur = conn.cursor()

	return cur, conn


# In[4]:


def drop_tables(cur, conn):
	for query in drop_table_queries:
		cur.execute(query)
		conn.commit()


# In[5]:


def create_tables(cur, conn):
	for query in create_table_queries:
		cur.execute(query)
		conn.commit()


# #### Extracting & Transforming Our Datasets

# In[6]:


AccountsCountry = pd.read_csv("Data/Wealth-AccountsCountry.csv")


# In[7]:


AccountsCountry.head()


# In[8]:


AccountsCountry.rename(columns={'Code': 'Country Code'}, inplace=True)


# In[9]:


AccountsCountry.head()


# In[10]:


AccountsCountry_clean = AccountsCountry[['Country Code', 'Short Name', 'Table Name', 'Long Name', 'Currency Unit']]


# In[11]:


AccountsCountry_clean.head()


# In[12]:


AccountsData = pd.read_csv("Data/Wealth-AccountData.csv")


# In[13]:


AccountsData.head()


# In[14]:


# let's see the columns
AccountsData.columns


# In[15]:


#let's trim down the number of columns
AccountsData = AccountsData[['Country Name', 'Country Code', 'Series Name', 'Series Code','2018 [YR2018]', '2017 [YR2017]','2016 [YR2016]','2015 [YR2015]','2014 [YR2014]']]


# In[16]:


#let's rename our numeric columns
AccountsData.rename(columns={'2018 [YR2018]': 'year_2018', '2017 [YR2017]': 'year_2017', '2016 [YR2016]': 'year_2016', '2015 [YR2015]': 'year_2015', '2014 [YR2014]': 'year_2014'}, inplace=True)


# In[17]:


# let's verify if numeric columns were renamed successfully
AccountsData.columns


# In[18]:


AccountsSeries = pd.read_csv("Data/Wealth-AccountSeries.csv")


# In[19]:


AccountsSeries.head()


# In[20]:


# rename cloumn from code to Series Code
AccountsSeries.rename(columns={'Code': 'Series Code'}, inplace=True)


# In[21]:


# verify if column has been renamed
AccountsSeries.columns


# In[22]:


AccountsSeries = AccountsSeries[['Series Code','Topic','Indicator Name', 'Long definition']]


# In[23]:


AccountsSeries.head()


# #### Creating tables in the accounts Database

# In[24]:


cur, conn = create_database()


# In[25]:


acc_table_create = ("""CREATE TABLE IF NOT EXISTS accountscountry(
country_code VARCHAR PRIMARY KEY,
short_name VARCHAR,
table_name VARCHAR,
long_name VARCHAR,
currency_unit VARCHAR
)""")


# In[26]:


cur.execute(acc_table_create)
conn.commit()


# In[27]:


accounts_data_table_create = ("""CREATE TABLE IF NOT EXISTS accountsdata(
country_name VARCHAR,
country_code VARCHAR,
series_name VARCHAR,
series_code VARCHAR,
year_2018 numeric,
year_2017 numeric,
year_2016 numeric,
year_2015 numeric,
year_2014 numeric
)""")


# In[28]:


cur.execute(accounts_data_table_create)
conn.commit()


# In[29]:


# create the accountseries table
accountseries_data_table_create = ("""CREATE TABLE IF NOT EXISTS accountseries(
series_code VARCHAR,
topic VARCHAR,
indicator_name VARCHAR, 
long_definition VARCHAR
)""")


# In[30]:


cur.execute(accountseries_data_table_create)
conn.commit()


# #### Inserting data into our tables

# In[31]:


#Insert into accounts country table
acc_table_insert = ("""INSERT INTO accountscountry(
country_code,
short_name,
table_name,
long_name,
currency_unit)
VALUES(%s, %s, %s, %s, %s)
""")


# In[32]:


# check for duplicates
AccountsCountry_clean.duplicated().sum()


# In[33]:


# Remove duplicates
AccountsCountry_clean = AccountsCountry_clean.drop_duplicates()
AccountsCountry_clean.duplicated().sum()


# In[34]:


#check for null values
AccountsCountry_clean.isna().sum()


# In[35]:


#remove null values
AccountsCountry_clean.dropna(subset={'Country Code','Short Name', 'Table Name', 'Long Name', 'Currency Unit'}, inplace=True)


# In[36]:


#verify if null still exist after dropping them.
AccountsCountry_clean.isna().sum()


# In[37]:


#Inserting rows from our accounts country dataframe
for i, row in AccountsCountry_clean.iterrows():
	cur.execute(acc_table_insert, list(row))


# In[38]:


conn.commit()


# In[39]:


# insert the accountseries table
accountseries_data_table_insert = ("""INSERT INTO accountseries(
series_code,
topic,
indicator_name, 
long_definition 
)
VALUES (%s, %s, %s, %s)
""")


# In[40]:


#Inserting rows from our accounts series dataframe
for i, row in AccountsSeries.iterrows():
	cur.execute(accountseries_data_table_insert, list(row))


# In[41]:


conn.commit()


# In[ ]:




