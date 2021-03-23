import pandas as pd
import cx_Oracle
import urllib.request
import requests
from requests_futures.sessions import FuturesSession
import validators as vd
import asyncio
import aiohttp
import nest_asyncio


dbConnection = cx_Oracle.connect("yourusername", "PASSWORD", "DATABASE_NAME", encoding="UTF-8",nencoding='UTF16') #You can use any client 
query = "SELECT * FROM TABLE"  #To get the URLS from the database
queryResultDF = pd.read_sql_query(query,dbConnection)


#To attache 'http://' string to the domain names.
def make_url(url):
    d_url = 'http://'+str(url)
    return d_url

async def process_url(df, url,session):
    try:
        async with session.get(url) as response:
            status_code = response.status
            df.loc[(df['COMPANY_URL'] == url) & (df['STATUS'] == 'NOT_FOUND'),['STATUS']] = status_code
            print(status_code,url)
    except :
        df.loc[(df['COMPANY_URL'] == url) & (df['STATUS'] == 'NOT_FOUND'),['STATUS']] = 408
        print('Connection Error',url)
        

#sock_connect The maximal number of seconds for connecting to a peer for a new connection, not given from a pool.
async def main(df,loop):
    timeout = aiohttp.ClientTimeout(sock_connect=25)
    async with aiohttp.ClientSession(timeout=timeout,loop=loop) as session:
        await asyncio.gather(*(process_url(df, url,session) for url in df[(df['STATUS'] == 'NOT_FOUND')]['COMPANY_URL']))
        print(df)


loop = asyncio.get_event_loop()
loop.run_until_complete(main(df_1,loop)) #df1 is the dataframe to be passed.        