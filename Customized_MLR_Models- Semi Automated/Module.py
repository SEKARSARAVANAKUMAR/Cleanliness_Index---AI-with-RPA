from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import pandas as pd 
import numpy as np
from datetime import datetime, timedelta
from time import gmtime, strftime
import pymysql
import sys
import requests
import sqlalchemy
import mysql

def DataBase_Connection():
    database_username = '****'
    database_password = '****'
    database_ip       = '****'
    database_name     = '*****'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(database_username, database_password, database_ip, database_name))
    return database_connection 


def Total_Usage_Preparation_hrly(Paper,Toilet,Wash):
    Paper_Usage=pd.DataFrame(Paper.groupby(['client_id','area_id','date_time','period'])['usage'].sum())
    Paper_Usage.reset_index(inplace=True)
    Paper_Usage.columns=['client_id','area_id','date_time','period','Paper_usage']
    Toilet_Usage=pd.DataFrame(Toilet.groupby(['client_id','area_id','date_time','period'])['usage'].sum())
    Toilet_Usage.reset_index(inplace=True)    
    Wash_Usage=pd.DataFrame(Wash.groupby(['client_id','area_id','date','period'])['water_usage','traffic_count'].sum())
    Wash_Usage.reset_index(inplace=True)
    Toilet_Usage.columns=['client_id','area_id','date_time','period','Toilet_usage']
    Paper_Toilet=pd.merge(Paper_Usage,Toilet_Usage,on=['client_id','area_id','date_time','period'],how='outer')
    Paper_Toilet=Paper_Toilet.sort_values(['client_id','area_id','date_time','period'],ascending=True)
    Paper_Toilet.date_time=pd.to_datetime(Paper_Toilet.date_time)
    Paper_Toilet['period']=Paper_Toilet.date_time.apply(lambda x: x.hour)
    Paper_Toilet.date_time=Paper_Toilet.date_time.astype(str)
    new=Paper_Toilet.date_time.str.split(" ",expand=True)
    Paper_Toilet['date']=new[0]    
    Paper_Toilet_Usage=pd.DataFrame(Paper_Toilet.groupby(['client_id','area_id','date','period'])['Paper_usage','Toilet_usage'].sum())
    Final=pd.merge(Wash_Usage,Paper_Toilet_Usage,on=['client_id','area_id','date','period'],how='outer')
    Final=Final.sort_values(['client_id','area_id','date','period'],ascending=True)
    return Final


def Total_Usage_Preparation_half_Hrly(Paper,Toilet,Wash):   
    Paper_Usage=pd.DataFrame(Paper.groupby(['client_id','area_id','date_time','period'])['usage'].sum())
    Paper_Usage.reset_index(inplace=True)
    Paper_Usage.columns=['client_id','area_id','date_time','period','Paper_usage']
    Toilet_Usage=pd.DataFrame(Toilet.groupby(['client_id','area_id','date_time','period'])['usage'].sum())
    Toilet_Usage.reset_index(inplace=True)    
    Wash_Usage=pd.DataFrame(Wash.groupby(['client_id','area_id','date','period'])['water_usage','traffic_count'].sum())
    Wash_Usage.reset_index(inplace=True)
    Toilet_Usage.columns=['client_id','area_id','date_time','period','Toilet_usage']
    Paper_Toilet=pd.merge(Paper_Usage,Toilet_Usage,on=['client_id','area_id','date_time','period'],how='outer')
    Paper_Toilet=Paper_Toilet.sort_values(['client_id','area_id','date_time','period'],ascending=True)
    new=Paper_Toilet.date_time.str.split(" ",expand=True)
    Paper_Toilet['date']=new[0]
    Paper_Toilet_Usage=pd.DataFrame(Paper_Toilet.groupby(['client_id','area_id','date','period'])['Paper_usage','Toilet_usage'].sum())
    Paper_Toilet_Usage=Paper_Toilet_Usage.sort_values(['client_id','area_id','date','period'],ascending=True)
    Paper_Toilet_Usage.reset_index(inplace=True)
    return Paper_Toilet_Usage




def Pca_Model_Loading(X,areaId,c_id,pub_time):
    sc = StandardScaler()
    X_scaled = sc.fit_transform(X)
    pca = PCA(n_components = X.shape[1])
    X_pca = pca.fit_transform(X_scaled)
    k=pca.components_
    gh=pd.DataFrame(k[0])
    gh=gh
    if X.shape[1] == 2:
        principalDf = pd.DataFrame(data = X_pca,columns = ['principal1', 'principal2'])
    elif X.shape[1]==3:
        principalDf = pd.DataFrame(data = X_pca,columns = ['principal1', 'principal2','principal3'])
    elif X.shape[1]==4:
        principalDf = pd.DataFrame(data = X_pca,columns = ['principal1', 'principal2','principal3','principal4'])
    else:
        principalDf = pd.DataFrame(data = X_pca,columns = ['principal1', 'principal2','principal3','principal4','principal5'])
    zo=principalDf['principal1'].min()
    lo=principalDf['principal1'].max()
    gh['variation_cap']=pca.explained_variance_ratio_[0]
    gh['loading_factor_Min']=zo
    gh['loading_factor_Max']=lo
    gh['area_id']=areaId
    gh['client_id']=c_id
    gh['pub_time']=pub_time
    gh['created_date']=pd.to_datetime('now').replace(microsecond=0)
    gh.columns=['loadings','variation_cap','loading_factor_Min','loading_factor_Max','area_id', 'client_id', 'pub_time', 'created_date']
    r=X.keys()
    gh['device_type']=r
    gh['device_type']=gh['device_type'].replace(['Toilet_usage','Paper_usage','Trash_Usage','traffic_count','water_usage','traffic_Count'],('toiletPaper','paperTowel','trashbin','trafficCount','waterUsage','trafficCount'))  
    return gh



def Min_Max_Calculation(X,areaId,c_id,pub_time):
    new_Var = X
    upper_quartile = np.percentile(new_Var, 75)
    lower_quartile = np.percentile(new_Var, 25)
    iqr = upper_quartile - lower_quartile
    maxi =new_Var[new_Var<=upper_quartile+1.5*iqr].max()
    mini = new_Var[new_Var>=lower_quartile-1.5*iqr].min()
    #maxi=X.max()
    #mini=X.min()
    mx=pd.DataFrame(maxi)
    mx.reset_index(inplace=True)
    mx.columns=['device_type','maximum']
    mn=pd.DataFrame(mini)
    mn.reset_index(inplace=True)
    mn.columns=['device_type','minimum']
    Min_Max=pd.merge(mx,mn,on=['device_type'],how='outer')
    std=pd.DataFrame(X.std())
    std.reset_index(inplace=True)
    std.columns=['device_type','std_dev']
    mean=pd.DataFrame(X.mean())
    mean.reset_index(inplace=True)
    mean.columns=['device_type','mean']
    Min_Max_=pd.merge(std,mean,on=['device_type'],how='outer')
    Min_Max_Cal=pd.merge(Min_Max,Min_Max_,on=['device_type'],how='outer')
    Min_Max_Cal['device_type']=Min_Max_Cal['device_type'].replace(['Toilet_usage','Paper_usage','Trash_Usage','traffic_count','water_usage','traffic_Count'],('toiletPaper','paperTowel','trashbin','trafficCount','waterUsage','trafficCount'))
    Min_Max_Cal['area_id']=areaId
    Min_Max_Cal['client_id']=c_id
    Min_Max_Cal['processing_id']=None
    Min_Max_Cal['device_type_id']=Min_Max_Cal['device_type'].replace(['toiletPaper','paperTowel','trashbin','trafficCount','waterUsage'],('2','1','3','4','5'))
    return Min_Max_Cal




def PCA_loading_factor(Final,loading_Factor):
    lf_toilet=loading_Factor[(loading_Factor.device_type.str.contains('toiletPaper'))]
    lf_paper=loading_Factor[(loading_Factor.device_type.str.contains('paperTowel'))]
    lf_water=loading_Factor[(loading_Factor.device_type.str.contains('water'))]
    lf_traffic=loading_Factor[(loading_Factor.device_type.str.contains('traffic'))]
    toilet_std_N=((int(Final['Toilet_usage']) - lf_toilet['mean'])/lf_toilet['std_dev'])
    paper_std_N=((int(Final['Paper_usage']) - lf_paper['mean'])/lf_paper['std_dev'])
    water_std_N=((int(Final['water_usage']) - lf_water['mean'])/lf_water['std_dev'])
    traffic_std_N=((int(Final['traffic_count']) - lf_traffic['mean'])/lf_traffic['std_dev'])
    Final['scoring']=(float(lf_toilet.loadings*toilet_std_N))+(float(lf_paper.loadings*paper_std_N))+(float(lf_water.loadings*water_std_N))+(float(lf_traffic.loadings*traffic_std_N))
    Final['Unclealiness_Index']=(Final['scoring'] - lf_toilet['loading_factor_Min'])/(lf_toilet['loading_factor_Max']-lf_toilet['loading_factor_Min'])
    Final=Final[['client_id', 'area_id', 'date', 'period', 'scoring','Unclealiness_Index','created_date']]
    return Final
def PCA_loading_factor_half_hour(Final,loading_Factor):
    lf_toilet=loading_Factor[(loading_Factor.device_type.str.contains('toiletPaper'))]
    lf_paper=loading_Factor[(loading_Factor.device_type.str.contains('paperTowel'))]
    #lf_water=loading_Factor[(loading_Factor.device_type.str.contains('water'))]
    lf_traffic=loading_Factor[(loading_Factor.device_type.str.contains('traffic'))]
    lf_trash=loading_Factor[(loading_Factor.device_type.str.contains('trashbin'))]
    lf_traffic=lf_traffic.iloc[0:]
    toilet_std_N=((int(Final['Toilet_usage']) - lf_toilet['mean'])/lf_toilet['std_dev'])
    paper_std_N=((int(Final['Paper_usage']) - lf_paper['mean'])/lf_paper['std_dev'])
    Traffic_std_N=((float(Final['Traffic_count']) - lf_traffic['mean'])/lf_traffic['std_dev'])
    trash_std_N=((float(Final['Trash_usage']) - lf_trash['mean'])/lf_trash['std_dev'])
    #water_std_N=((int(Final['water_usage']) - lf_water['mean'])/lf_water['std_dev'])
    #traffic_std_N=((int(Final['traffic_count']) - lf_traffic['mean'])/lf_traffic['std_dev'])
    Final['scoring']=(float(lf_toilet.loadings*toilet_std_N))+(float(lf_paper.loadings*paper_std_N))+(float(lf_traffic.loadings*Traffic_std_N))+(float(lf_trash.loadings*trash_std_N))
    Final['Unclealiness_Index']=(Final['scoring'] - lf_toilet['loading_factor_Min'])/(lf_toilet['loading_factor_Max']-lf_toilet['loading_factor_Min'])
    Final=Final[['client_id', 'area_id', 'date', 'date_time', 'scoring','Unclealiness_Index','created_date']]
    return Final

def Total_Usage_Preparation_half_Hrly_with_Traffic(Paper,Toilet,Wash,Traffic,Trash):   
    Paper_Usage=pd.DataFrame(Paper.groupby(['client_id','area_id','date_time','period'])['usage'].sum())
    Paper_Usage.reset_index(inplace=True)
    Paper_Usage.columns=['client_id','area_id','date_time','period','Paper_usage']
    Toilet_Usage=pd.DataFrame(Toilet.groupby(['client_id','area_id','date_time','period'])['usage'].sum())
    Toilet_Usage.reset_index(inplace=True)    
    Toilet_Usage.columns=['client_id','area_id','date_time','period','Toilet_usage']
    Trash_Usage=pd.DataFrame(Trash.groupby(['client_id','area_id','date_time','period'])['usage'].sum())
    Trash_Usage.reset_index(inplace=True)    
    Trash_Usage.columns=['client_id','area_id','date_time','period','Trash_Usage']
    Wash_Usage=pd.DataFrame(Wash.groupby(['client_id','area_id','date','period'])['water_usage','traffic_count'].sum())
    Wash_Usage.reset_index(inplace=True)
    Paper_Toilet=pd.merge(Paper_Usage,Toilet_Usage,on=['client_id','area_id','date_time','period'],how='outer')
    Paper_Toilet_Trash=pd.merge(Paper_Toilet,Trash_Usage,on=['client_id','area_id','date_time','period'],how='outer')
    Traffic_Count=pd.DataFrame(Traffic.groupby(['client_id','area_id','date_time','period'])['traffic_count'].sum())
    Traffic_Count.reset_index(inplace=True)    
    Traffic_Count.columns=['client_id','area_id','date_time','period','traffic_count']
    Paper_Toilet_Traffic=pd.merge(Paper_Toilet_Trash,Traffic_Count,on=['client_id','area_id','date_time','period'],how='outer')
    Paper_Toilet_Traffic=Paper_Toilet_Traffic.sort_values(['client_id','area_id','date_time','period'],ascending=True)
    new=Paper_Toilet_Traffic.date_time.str.split(" ",expand=True)
    Paper_Toilet_Traffic['date']=new[0]
    Paper_Toilet_Usage=pd.DataFrame(Paper_Toilet_Traffic.groupby(['client_id','area_id','date','period'])['Paper_usage','Toilet_usage','Trash_Usage','traffic_count'].sum())
    Paper_Toilet_Usage=Paper_Toilet_Usage.sort_values(['client_id','area_id','date','period'],ascending=True)
    Paper_Toilet_Usage.reset_index(inplace=True)
    return Paper_Toilet_Usage
def Total_Paper_Usage(output):
    output.usage=output.usage.astype(float)
    Paper_Usage=pd.DataFrame(output.groupby(['client_id','area_id','date','date_time'])['usage'].sum())
    Paper_Usage.reset_index(inplace=True)
    Paper_Usage.columns=['client_id', 'area_id', 'date', 'date_time', 'Paper_usage']
    Paper_Usage['created_date']=pd.to_datetime('now').replace(microsecond=0)
    return Paper_Usage

def Total_Toilet_Usage(output):
    output.usage=output.usage.astype(float)
    Toilet_Usage=pd.DataFrame(output.groupby(['client_id','area_id','date','date_time'])['usage'].sum())
    Toilet_Usage.reset_index(inplace=True)
    Toilet_Usage.columns=['client_id', 'area_id', 'date', 'date_time', 'Toilet_usage']
    Toilet_Usage['created_date']=pd.to_datetime('now').replace(microsecond=0)
    return Toilet_Usage
def Total_Trash_Usage(output):
    output.usage=output.usage.astype(float)
    Toilet_Usage=pd.DataFrame(output.groupby(['client_id','area_id','date','date_time'])['usage'].sum())
    Toilet_Usage.reset_index(inplace=True)
    Toilet_Usage.columns=['client_id', 'area_id', 'date', 'date_time', 'Trash_usage']
    Toilet_Usage['created_date']=pd.to_datetime('now').replace(microsecond=0)
    return Toilet_Usage
def Total_Washbasin_Usage(output):
    Wash_Usage=pd.DataFrame(output.groupby(['client_id','area_id','date','period'])['water_usage','traffic_count'].sum())
    Wash_Usage.reset_index(inplace=True)
    Wash_Usage.columns=['client_id', 'area_id', 'date', 'period', 'water_usage','traffic_count']
    Wash_Usage['created_date']=pd.to_datetime('now').replace(microsecond=0)
    return Wash_Usage
def get_period(dt):
    hh = int(dt.split(":")[0])
    jj= int(dt.split(":")[1])
    dl=[]
    if hh == 0 :
        ad = 0
        #ls.append(ad)
        if jj==30:
            ad = ad+1
            dl.append(ad)
        else:
            dl.append(ad)
    elif hh > 0 :
        added=hh*2
        #dl.append(added)
        if jj==30:
            added=added+1
            dl.append(added)
        else:
            dl.append(added)
    return dl


