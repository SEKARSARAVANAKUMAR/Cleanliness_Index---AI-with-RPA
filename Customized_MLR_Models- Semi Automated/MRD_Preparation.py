import pymysql
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from time import gmtime, strftime
import sys
import requests
import sqlalchemy
import mysql
db = pymysql.connect("****", "****", "***", "***")

#s_cn=str(input("enter client_id ex: 11 for denver"))
a = None
try:
    query1="select * from sample_client_details where clientid=17  and areaid=26;"# where clientid ="+s_cn+";"
    print("db connection sucessfull")
    with db.cursor() as cursor:
        cursor.execute(query1)
        a=pd.read_sql(query1,db)
        print(a.info())
        #print(df.describe(include='all'))
except:
    print("Sample_client_details query error")

a.devicetype =a.devicetype.astype(str)
z=a[(a.devicetype.str.contains('PeopleCount'))]
z.reset_index(inplace=True)
#z=z[z.pub_time == 30]

for index,rows in z.iterrows():
    #print(rows)
    c_id=rows.clientid
    cn=rows.clinetname
    areaId= rows.areaid
    dev_type=rows.devicetype
    ct=rows.timezone
    tf=rows.timeLF
    tfd=rows.timeFD
    pub_time=rows.pub_time
    
    #Final_1 = None
    ct_=int(ct)
    dt_client = datetime.utcnow() - timedelta(hours=ct_)
    #dt_client=pd.to_datetime('2019-06-30 22:35:00')
    dt_client_=dt_client - timedelta(minutes=150)
    sd=dt_client_.strftime('%Y-%m-%d %H:%M:%S')
    ed=dt_client.strftime('%Y-%m-%d %H:%M:%S')
    #sd='2019-06-01 00:30:01'
    #ed='2019-06-01 01:00:00'
    
    new=sd.split(" ")
    s_Date =new[0]
    s_time = new[1]
    new=s_time.split(":")
    s_hour=new[0]
    s_minute=new[1]
    if s_minute>='29':
        s_minute='30'
    else:
        s_minute='00'
    s_date_time=s_Date+" "+s_hour+":"+s_minute+":00"
    new=ed.split(" ")
    e_Date =new[0]
    e_time = new[1]
    new=e_time.split(":")
    e_hour=new[0]
    e_minute=new[1]
    if e_minute>='29':
        e_minute='30'
    else:
        e_minute='00'
    
    e_date_time=e_Date+" "+e_hour+":"+e_minute+":00"
    try:
        query1="select * from Aggregated_usage_stream where date_time >='"+s_date_time+"' and date_time <= '"+e_date_time+"' and client_id = "+c_id+" and area_id= "+areaId+";"            #print("db connection sucessfull")
        with db.cursor() as cursor:
            cursor.execute(query1)
            agg_usage=pd.read_sql(query1,db)
    except:
        print("check the data")
    try:
        paper_lag=agg_usage[['client_id','area_id','date_time','period_type','period','paper_usage']]
        paper_lag['paper_usage_lag1']=paper_lag.paper_usage.shift(1)
        paper_lag['paper_usage_lag2']=paper_lag.paper_usage_lag1.shift(1)
        paper_lag['paper_usage_lag3']=paper_lag.paper_usage_lag2.shift(1)
        paper_lag['paper_usage_lag4']=paper_lag.paper_usage_lag3.shift(1)
        paper_lag['paper_usage_lag5']=paper_lag.paper_usage_lag4.shift(1)
        paper_lag=paper_lag.iloc[-1:]
        tlt_paper_lag=agg_usage[['client_id','area_id','date_time','period_type','period','tlt_paper_usage']]
        tlt_paper_lag['tlt_paper_usage_lag1']=tlt_paper_lag.tlt_paper_usage.shift(1)
        tlt_paper_lag['tlt_paper_usage_lag2']=tlt_paper_lag.tlt_paper_usage_lag1.shift(1)
        tlt_paper_lag['tlt_paper_usage_lag3']=tlt_paper_lag.tlt_paper_usage_lag2.shift(1)
        tlt_paper_lag['tlt_paper_usage_lag4']=tlt_paper_lag.tlt_paper_usage_lag3.shift(1)
        tlt_paper_lag['tlt_paper_usage_lag5']=tlt_paper_lag.tlt_paper_usage_lag4.shift(1)
        tlt_paper_lag=tlt_paper_lag.iloc[-1:]
        trash_lag=agg_usage[['client_id','area_id','date_time','period_type','period','trash_usage']]
        trash_lag['trash_usage_lag1']=trash_lag.trash_usage.shift(1)
        trash_lag['trash_usage_lag2']=trash_lag.trash_usage_lag1.shift(1)
        trash_lag['trash_usage_lag3']=trash_lag.trash_usage_lag2.shift(1)
        trash_lag['trash_usage_lag4']=trash_lag.trash_usage_lag3.shift(1)
        trash_lag['trash_usage_lag5']=trash_lag.trash_usage_lag4.shift(1)
        trash_lag=trash_lag.iloc[-1:]
        paper_tlt=pd.merge(paper_lag,tlt_paper_lag,on=['client_id','area_id','date_time','period_type','period'],how='right')
        paper_tlt_trash=pd.merge(paper_tlt,trash_lag,on=['client_id','area_id','date_time','period_type','period'],how='right')
    except:
        print("check the last five period data",areaId)
    try:
        database_username = '***'
        database_password = '****'
        database_ip       = '***'
        database_name     = '***'
        database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(database_username, database_password, database_ip, database_name))
        paper_tlt_trash.to_sql(con=database_connection, name='MRD_Preparation', if_exists='append',index=False)
    except:
        print("check the connections")
db.close()