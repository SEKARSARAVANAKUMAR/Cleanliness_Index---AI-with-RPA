import Model_Module_batch
import pymysql
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from time import gmtime, strftime
import sys
import requests
import sqlalchemy
import mysql
import statsmodels.api as sm
import statsmodels.formula.api as smf

db = pymysql.connect("****", "***", "***", "***")
dbe = pymysql.connect("****", "***", "***", "***")

#s_cn=str(input("enter client_id ex: 11 for denver"))
a = None
try:
    query1="select * from sample_client_details where clientid=11;"
    print("db connection sucessfull")
    with db.cursor() as cursor:
        cursor.execute(query1)
        a=pd.read_sql(query1,db)
        print(a.info())
        #print(df.describe(include='all'))
except:
    print("Sample_client_details query error")


a.devicetype =a.devicetype.astype(str)
z=a[(a.devicetype.str.contains('paperTowel'))]
z.reset_index(inplace=True)
#z=z[z.pub_time == 30]

for index,rows in z.iterrows():
    #print(rows)
    c_id=rows.clientid
    areaId= rows.areaid
    pub_time=rows.pub_time
    prss_id=rows.processing_id
    client_type=rows.type
    a=None
    try:
        query1="SELECT * FROM people_count_batch where client_id="+c_id+" and area_id="+areaId+";"
        query2="select * from model_type where model_type='lm_traffic';"
        query3="select * from feature_dim where feature_name='papertowel';"
        query4="SELECT * FROM paper_towel_batch where client_id="+c_id+" and area_id="+areaId+";"
        #query="select * from client_table;"
        with db.cursor() as cursor:
            cursor.execute(query1)
            traffic=pd.read_sql(query1,db)
            cursor.execute(query2)
            model_type=pd.read_sql(query2,db)
            cursor.execute(query3)
            feature_dim=pd.read_sql(query3,db)
            cursor.execute(query4)
            paper_towel=pd.read_sql(query4,db)
    except:
        print("Batch_Data query error")
    try:
        try:
            agg_tra=Model_Module_batch.agg_traffic(traffic)
            agg_usa=Model_Module_batch.agg_usage(paper_towel)
            agg_tra['date_time'] = agg_tra['date_time'].astype(str)
            agg_usa['date_time'] = agg_usa['date_time'].astype(str)
            final=pd.merge(agg_tra,agg_usa, on =['date_time'], how='left')
            #training set
            train_set = final[(final['date_time'] > '2019-05-01 00:00:00') & (final['date_time'] <= '2019-08-31 23:30:00')]
            train_set.reset_index(inplace=True)
        except:
            agg_usa=Model_Module_batch.agg_usage2(paper_towel)
            #agg_tra['date_time'] = agg_tra['date_time'].astype(str)
            agg_usa['date_time'] = agg_usa['date_time'].astype(str)
            #final=pd.merge(agg_tra,agg_usa, on =['date_time'], how='left')
            #training set
            train_set = agg_usa[(agg_usa['date_time'] > '2019-05-01 00:00:00') & (agg_usa['date_time'] <= '2019-08-31 23:30:00')]
            train_set.reset_index(inplace=True)
        trainning_set,testing_set=Model_Module_batch.Model_Data_Formation(train_set)
        model_lm2,feature_sel,best_parameter,model_lm1=Model_Module_batch.Model_Building_usage(trainning_set)
        Mape,Mape_R=Model_Module_batch.Model_Evaluation_Testing(best_parameter,model_lm2,testing_set,trainning_set)
        best_parameter.reset_index(inplace=True)
        params2=pd.DataFrame(model_lm2.params,columns=['coef'])
        params2=params2.T
        params2.reset_index(inplace=True)
        params2=params2.drop(['index'],axis=1)
        params2['processing_id']=prss_id
        params2['model_type_id']=model_type.model_type_id
        params2['client_id']=c_id
        params2['area_id']=areaId
        params2['feature_id']=feature_dim.feature_id
        params2['feature']=feature_dim.feature_name
        params2['r_squared']=model_lm2.rsquared
        params2['Adj_Rsquared']=model_lm2.rsquared_adj
        params2['MAPE_Test']=Mape
        params2['MAPE_Train']=Mape_R
        params2['ap_param']=best_parameter.opt_parms
        db_con=Model_Module_batch.DataBase_Connection()
        params2.to_sql(con=db_con, name='lm_coef_modified', if_exists='append',index=False)
    except:
        print("check the data format",areaId)
    #break
db.close()
dbe.close()