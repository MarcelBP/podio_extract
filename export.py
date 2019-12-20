#!/usr/bin/env python3
import sys
import fastavro
import pandas as pd 
from pypodio2 import api
import json
from io import BytesIO 
from sqlalchemy import create_engine
import sqlalchemy
from sqlalchemy import types
import pickle 
import os
from time import sleep


c = api.OAuthClient(
    "",
    "",
    "",
    "",)

def sqlcol(dfparam):    
    dtypedict = {}
    for i,j in zip(dfparam.columns, dfparam.dtypes):
        if j == "object":
            dtypedict.update({i: sqlalchemy.types.TEXT})
    return dtypedict

def sane_columns(data):
    data.columns = data.columns.str.strip().str.lower().str.replace('/','_').str.replace(' ', '_').str.replace('(', '_')
    data.columns = data.columns.str.replace(':','_').str.replace('&', '_').str.replace('*', '_').str.replace('@','_')
    data.columns = data.columns.str.replace('#', '_').str.replace('$', '_').str.replace('%', '_').str.replace(')', '_')
    data.columns = data.columns.str.replace('-', '_').str.replace('^', '_').str.replace(';', '_').str.replace('!', '_')
    data.columns = data.columns.str.replace('.', '_').str.replace('^', '_').str.replace(';', '_').str.replace('!', '_')
    data.columns = data.columns.str.replace('?', '_').str.replace(',','_').str.replace('___', '_').str.replace('__', '_')
    data.columns = data.columns.str[:50]
    return data.columns

def sync_mysql(data, engine):
    data.to_sql("", engine, index=False, if_exists='replace', dtype=sqlcol(data))
    connection = engine.connect()
    result = connection.execute("replace into x select * from x")

def fetch(x,z=30):
    y = pickle.load(open('/tmp3/last_orders.offset','rb'))
    if x < y:
        print ("Skipping % ", x)
        return
    sleep(z)
    print(x)
    try:
        offsets = pickle.load(r())
        data = pd.read_excel(BytesIO(c.Item.xlsx(app, limit=1000, offset=x)))
        data.columns = sane_columns(data)
        data = data.loc[:,~data.columns.duplicated()]
        sync_mysql(data, engine)
        offsets.remove(x)
        pickle.dump(x, open('/tmp3/last_orders.offset','wb'))
        print ("Retrived % ", x)
        pickle.dump(offsets, w())
        del data
        return x
    except Exception as e:
        print(e)
        z = z + 15 if z < 180 else 180 
        fetch(x,z)

def w():
    return open('/tmp3/orders.offsets', 'wb')

def r():
    return open('/tmp3/orders.offsets', 'rb')

def get_offsets():
    try:   
        count = int(sys.argv[1]) if len(sys.argv) >= 2 else c.Item.count(app)['count']
        offsets = list(range(0, count, 1000))
        pickle.dump(offsets, w())
        return offsets
    except Exception as e:
        print(e)
    
if __name__ == "__main__":
    app = int(sys.argv[3]) if len(sys.argv) >= 4 else 1 
    try:
        f = open("/tmp3/orders.lock")
    except Exception as e:
        i = int(sys.argv[2]) if len(sys.argv) >= 3 else 0
        pickle.dump(i, open('/tmp3/last_orders.offset', 'wb'))
        pickle.dump(i, open('/tmp3/orders.lock', 'wb'))
        offsets = get_offsets()
        pickle.dump(offsets, w())
        print(offsets[-1])
        engine = create_engine('mysql+pymysql://a1:a1@localhost:3306/test')
        [fetch(x) for x in offsets]
        #pickle.dump(df,open('/tmp3/orders_finished.offsets', 'wb'))
        #df.to_csv('orders.csv')
        os.remove("/tmp3/orders.lock")
        os.remove("/tmp3/orders.offsets")
