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


def DataBase_Connection():
    database_username = '***'
    database_password = '****'
    database_ip       = '****'
    database_name     = '****'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(database_username, database_password, database_ip, database_name))
    return database_connection


def agg_traffic(traffic):
    traffic_summed=traffic[['date_time','traffic_count','period']]
    final = traffic_summed
    #Getting only day of week from datetime 
    final['date_time'] = final['date_time'].astype(str)
    date = final['date_time'].str.split(" ",expand=True)
    final['date']=date[0]
    final['time'] = date[1]
    final['weekday']= pd.to_datetime(final['date']).dt.dayofweek
    final= final[['date_time','period','weekday','traffic_count']]
    return final


def agg_usage2(paper_towel):
    usage=pd.DataFrame(paper_towel.groupby(['date_time','period'])['usage'].sum())
    usage.reset_index(inplace=True)
    #final = traffic_summed
    #Getting only day of week from datetime 
    usage['date_time'] = usage['date_time'].astype(str)
    date = usage['date_time'].str.split(" ",expand=True)
    usage['date']=date[0]
    usage['time'] = date[1]
    usage['weekday']= pd.to_datetime(usage['date']).dt.dayofweek
    usage= usage[['date_time','period','weekday','usage']]
    return usage


def agg_usage(paper_towel):
    usage=pd.DataFrame(paper_towel.groupby(['date_time'])['usage'].sum())
    usage.reset_index(inplace=True)
    return usage



def model_data_preparation(train_,client_type):
    train_.isnull().sum()
    train_.weekday=train_.weekday.fillna(method='bfill')
    train_=train_.fillna(0)
    train_['lag1'] = train_['traffic_count'].shift(1)
    train_['lag2'] = train_['lag1'].shift(1)
    train_['lag3'] = train_['lag2'].shift(1)
    train_['lag4'] = train_['lag3'].shift(1)
    train_['lag5'] = train_['lag4'].shift(1)
    train_=train_.fillna(0)
    try:
        train_.date_time=pd.to_datetime(train_['date_time'])
        if client_type =='Office':
            train_['is_holiday']=np.where(train_['weekday'] <=4 ,0.0,1.0)
        else:
            train_['is_holiday']=0.0
    except:
        print("holiday variable error")
    train_[['weekday','period']]=train_[['weekday','period']].astype('category')
    return train_

def lm_traffic_model_generation(train_,model_type,feature_dim,prss_id,c_id,areaId):
    mod1res=smf.ols(formula='traffic_count ~ weekday + period+lag1+lag2+lag3+lag4+lag5+is_holiday', data=train_).fit()
    coeff=pd.DataFrame(mod1res.params,columns=['coef'])
    coeff['pval']=mod1res.pvalues
    Intc=coeff.iloc[:1,:1]
    Intc.reset_index(inplace=True)
    coeff['coef']=coeff.coef.apply(lambda X : round(X,4))
    coeff['coef']=np.where(coeff.pval <0.05,coeff.coef,None)
    coeff=coeff.drop(['pval'],axis=1)
    #coeff['coef']= coeff['coef'].applymap('$ {:,.4f}'.format)
    lm_tra=coeff.T
    lm_tra.reset_index(inplace=True)
    lm_tra['Intercept']=Intc.coef
    lm_tra['processing_id']=prss_id
    lm_tra['model_type_id']=model_type.model_type_id
    lm_tra['client_id']=c_id
    lm_tra['area_id']=areaId
    lm_tra['feature_id']=feature_dim.feature_id
    lm_tra['feature']=feature_dim.feature_name
    lm_tra['weekday[T.0.0]']=lm_tra['weekday[T.1.0]']
    lm_tra['period[T.0]']=lm_tra['period[T.1]']
    lm_tra_=lm_tra[['processing_id', 'model_type_id', 'client_id', 'area_id', 'feature_id',
       'feature','Intercept', 'weekday[T.0.0]','weekday[T.1.0]', 'weekday[T.2.0]', 'weekday[T.3.0]',
       'weekday[T.4.0]', 'weekday[T.5.0]', 'weekday[T.6.0]', 'period[T.0]','period[T.1]',
       'period[T.2]', 'period[T.3]', 'period[T.4]', 'period[T.5]',
       'period[T.6]', 'period[T.7]', 'period[T.8]', 'period[T.9]',
       'period[T.10]', 'period[T.11]', 'period[T.12]', 'period[T.13]',
       'period[T.14]', 'period[T.15]', 'period[T.16]', 'period[T.17]',
       'period[T.18]', 'period[T.19]', 'period[T.20]', 'period[T.21]',
       'period[T.22]', 'period[T.23]', 'period[T.24]', 'period[T.25]',
       'period[T.26]', 'period[T.27]', 'period[T.28]', 'period[T.29]',
       'period[T.30]', 'period[T.31]', 'period[T.32]', 'period[T.33]',
       'period[T.34]', 'period[T.35]', 'period[T.36]', 'period[T.37]',
       'period[T.38]', 'period[T.39]', 'period[T.40]', 'period[T.41]',
       'period[T.42]', 'period[T.43]', 'period[T.44]', 'period[T.45]',
       'period[T.46]', 'period[T.47]', 'lag1', 'lag2', 'lag3', 'lag4', 'lag5','is_holiday']]
    lm_tra_.columns=['processing_id', 'model_type_id', 'client_id', 'area_id', 'feature_id',
       'feature','Intercept', 'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday','period_0','period_1','period_2','period_3','period_4','period_5','period_6','period_7','period_8','period_9','period_10','period_11','period_12','period_13','period_14','period_15','period_16','period_17','period_18','period_19','period_20','period_21','period_22','period_23','period_24','period_25','period_26','period_27','period_28','period_29','period_30','period_31','period_32','period_33','period_34','period_35','period_36','period_37','period_38','period_39','period_40','period_41','period_42','period_43','period_44','period_45','period_46','period_47','lag1', 'lag2', 'lag3', 'lag4', 'lag5','is_holiday']
    return lm_tra_,mod1res,coeff


def lm_usage_model_generation(train_,model_type,feature_dim,prss_id,c_id,areaId):
    mod1res=smf.ols(formula='usage ~ weekday + period+lag1+lag2+lag3+lag4+lag5+is_holiday', data=train_).fit()#.fit_regularized(method='elastic_net', maxiter=100, alpha=0.0, L1_wt=1.0)
    #mod1res=mod1res.fit(formula='usage ~ weekday + period+lag1+lag2+lag3+lag4+lag5+is_holiday', data=train_)
    #print("score of model",mod1res.rsquared)fit_regularized(method='coord_descent', maxiter=1000, alpha=0.0, L1_wt=1.0, start_params=None, cnvrg_tol=1e-08, zero_tol=1e-08, **kwargs)
    coeff=pd.DataFrame(mod1res.params,columns=['coef'])
    Intc=coeff.iloc[:1,:1]
    Intc.reset_index(inplace=True)
    coeff['coef']=coeff.coef.apply(lambda X : round(X,4))
    coeff['coef']=np.where(coeff.coef >0,coeff.coef,None)
    #coeff['coef']= coeff['coef'].applymap('$ {:,.4f}'.format)
    lm_tra=coeff.T
    lm_tra.reset_index(inplace=True)
    lm_tra['Intercept']=Intc.coef
    lm_tra['processing_id']=prss_id
    lm_tra['model_type_id']=model_type.model_type_id
    lm_tra['client_id']=c_id
    lm_tra['area_id']=areaId
    lm_tra['feature_id']=feature_dim.feature_id
    lm_tra['feature']=feature_dim.feature_name
    lm_tra['weekday[T.0.0]']=lm_tra['weekday[T.1.0]']
    lm_tra['period[T.0]']=lm_tra['period[T.1]']
    lm_tra_=lm_tra[['processing_id', 'model_type_id', 'client_id', 'area_id', 'feature_id',
       'feature','Intercept', 'weekday[T.0.0]','weekday[T.1.0]', 'weekday[T.2.0]', 'weekday[T.3.0]',
       'weekday[T.4.0]', 'weekday[T.5.0]', 'weekday[T.6.0]', 'period[T.0]','period[T.1]',
       'period[T.2]', 'period[T.3]', 'period[T.4]', 'period[T.5]',
       'period[T.6]', 'period[T.7]', 'period[T.8]', 'period[T.9]',
       'period[T.10]', 'period[T.11]', 'period[T.12]', 'period[T.13]',
       'period[T.14]', 'period[T.15]', 'period[T.16]', 'period[T.17]',
       'period[T.18]', 'period[T.19]', 'period[T.20]', 'period[T.21]',
       'period[T.22]', 'period[T.23]', 'period[T.24]', 'period[T.25]',
       'period[T.26]', 'period[T.27]', 'period[T.28]', 'period[T.29]',
       'period[T.30]', 'period[T.31]', 'period[T.32]', 'period[T.33]',
       'period[T.34]', 'period[T.35]', 'period[T.36]', 'period[T.37]',
       'period[T.38]', 'period[T.39]', 'period[T.40]', 'period[T.41]',
       'period[T.42]', 'period[T.43]', 'period[T.44]', 'period[T.45]',
       'period[T.46]', 'period[T.47]', 'lag1', 'lag2', 'lag3', 'lag4', 'lag5','is_holiday']]
    lm_tra_.columns=['processing_id', 'model_type_id', 'client_id', 'area_id', 'feature_id',
       'feature','Intercept', 'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday','period_0','period_1','period_2','period_3','period_4','period_5','period_6','period_7','period_8','period_9','period_10','period_11','period_12','period_13','period_14','period_15','period_16','period_17','period_18','period_19','period_20','period_21','period_22','period_23','period_24','period_25','period_26','period_27','period_28','period_29','period_30','period_31','period_32','period_33','period_34','period_35','period_36','period_37','period_38','period_39','period_40','period_41','period_42','period_43','period_44','period_45','period_46','period_47','lag1', 'lag2', 'lag3', 'lag4', 'lag5','is_holiday']
    print(mod1res)
    return lm_tra_,mod1res,coeff

def Model_Data_Formation(train_set):
    train_set['Mon']=np.where(train_set.weekday==0, 1,0)
    train_set['Tue']=np.where(train_set.weekday==1,1,0)
    train_set['Wed']=np.where(train_set.weekday==2,1,0)
    train_set['Thu']=np.where(train_set.weekday==3,1,0)
    train_set['Fri']=np.where(train_set.weekday==4,1,0)
    train_set['Sat']=np.where(train_set.weekday==5,1,0)
    train_set['Sun']=np.where(train_set.weekday==6,1,0)
    train_set['Is_holiday']=np.where((train_set.weekday==5)| (train_set.weekday==6), 1,0)
    new=train_set['date_time'].str.split(" ",expand=True)
    train_set['date']=new[0]
    train_set['Is_holiday']=np.where((train_set['date']=='2019-05-27')|(train_set['date']=='2019-07-04')|(train_set['date']=='2019-07-05'),1,train_set['Is_holiday'])
    train_set['lag1_usage'] = train_set['usage'].shift(1)
    train_set['lag2_usage'] = train_set['lag1_usage'].shift(1)
    train_set['lag3_usage'] = train_set['lag2_usage'].shift(1)
    train_set['lag4_usage'] = train_set['lag3_usage'].shift(1)
    train_set['lag5_usage'] = train_set['lag4_usage'].shift(1)
    try:
        train_set['lag1_traffic'] = train_set['traffic_count'].shift(1)
        train_set['lag2_traffic'] = train_set['lag1_traffic'].shift(1)
        train_set['lag3_traffic'] = train_set['lag2_traffic'].shift(1)
        train_set['lag4_traffic'] = train_set['lag3_traffic'].shift(1)
        train_set['lag5_traffic'] = train_set['lag4_traffic'].shift(1)
    except:
        print("no traffic data")
    train_set=train_set.fillna(0)
    trainning_set=train_set[(train_set['date_time'] > '2019-05-01 00:00:00') & (train_set['date_time'] <= '2019-08-22 23:30:00')]
    testing_set=train_set[(train_set['date_time'] > '2019-08-23 00:00:00') & (train_set['date_time'] <= '2019-08-31 23:30:00')]
    return trainning_set,testing_set


def Model_Building_usage(trainning_set_demo):
    r=np.arange(1,49)
    for i in range(1,len(r)):
        ki=str(i)
        trainning_set_demo["period_"+ki+""]=np.where(trainning_set_demo.period==i,1,0)  
    #trainning_set_demo['period']=trainning_set_demo['period'].astype('category')
    opt_parameter=[None,0.05,0.10,0.15,0.20,0.25,0.25,0.30,0.35,0.40,0.45,0.50,0.55,0.60,0.65,0.70,0.75,0.80,0.85,0.90,0.95]
    r_sq=[]
    op_par=[]
    for i in range(0,len(opt_parameter)):
        try:
            trainning_set_demo['usage_']=trainning_set_demo['usage']**opt_parameter[i]
        except:
            trainning_set_demo['usage_']=trainning_set_demo['usage']
        try:
            model_lm=smf.ols(formula='usage_ ~ period_1+period_2+period_3+period_4+period_5+period_6+period_7+period_8+period_9+period_10+period_11+period_12+period_13+period_14+period_15+period_16+period_17+period_18+period_19+period_20+period_21+period_22+period_23+period_24+period_25+period_26+period_27+period_28+period_29+period_30+period_31+period_32+period_33+period_34+period_35+period_36+period_37+period_38+period_39+period_40+period_41+period_42+period_43+period_44+period_45+period_46+period_47+Mon+Tue+Wed+Thu+Fri+Sat+Is_holiday+lag1_usage+lag2_usage+lag3_usage+lag4_usage+lag5_usage+lag1_traffic+lag2_traffic+lag3_traffic+lag4_traffic+lag5_traffic+traffic_count', data=trainning_set_demo).fit()
        except:
            model_lm=smf.ols(formula='usage_ ~ period_1+period_2+period_3+period_4+period_5+period_6+period_7+period_8+period_9+period_10+period_11+period_12+period_13+period_14+period_15+period_16+period_17+period_18+period_19+period_20+period_21+period_22+period_23+period_24+period_25+period_26+period_27+period_28+period_29+period_30+period_31+period_32+period_33+period_34+period_35+period_36+period_37+period_38+period_39+period_40+period_41+period_42+period_43+period_44+period_45+period_46+period_47+Mon+Tue+Wed+Thu+Fri+Sat+Is_holiday+lag1_usage+lag2_usage+lag3_usage+lag4_usage+lag5_usage', data=trainning_set_demo).fit()
        r_sq.append(model_lm.rsquared)
        op_par.append(opt_parameter[i])
    dm=pd.DataFrame(r_sq,columns=['r_squared'])
    dm['opt_parms']=op_par
    best_parameter=dm[dm.r_squared>=dm.r_squared.max()]
    try:
        trainning_set_demo['usage_']=trainning_set_demo['usage']**np.array(best_parameter.opt_parms)
    except:
        trainning_set_demo['usage_']=trainning_set_demo['usage']
    try:
        model_lm=smf.ols(formula='usage_ ~ period_1+period_2+period_3+period_4+period_5+period_6+period_7+period_8+period_9+period_10+period_11+period_12+period_13+period_14+period_15+period_16+period_17+period_18+period_19+period_20+period_21+period_22+period_23+period_24+period_25+period_26+period_27+period_28+period_29+period_30+period_31+period_32+period_33+period_34+period_35+period_36+period_37+period_38+period_39+period_40+period_41+period_42+period_43+period_44+period_45+period_46+period_47+Mon+Tue+Wed+Thu+Fri+Sat+Is_holiday+lag1_usage+lag2_usage+lag3_usage+lag4_usage+lag5_usage+lag1_traffic+lag2_traffic+lag3_traffic+lag4_traffic+lag5_traffic+traffic_count', data=trainning_set_demo).fit()
    except:
        model_lm=smf.ols(formula='usage_ ~ period_1+period_2+period_3+period_4+period_5+period_6+period_7+period_8+period_9+period_10+period_11+period_12+period_13+period_14+period_15+period_16+period_17+period_18+period_19+period_20+period_21+period_22+period_23+period_24+period_25+period_26+period_27+period_28+period_29+period_30+period_31+period_32+period_33+period_34+period_35+period_36+period_37+period_38+period_39+period_40+period_41+period_42+period_43+period_44+period_45+period_46+period_47+Mon+Tue+Wed+Thu+Fri+Sat+Is_holiday+lag1_usage+lag2_usage+lag3_usage+lag4_usage+lag5_usage', data=trainning_set_demo).fit()
    coeff=pd.DataFrame(model_lm.params,columns=['coef'])
    coeff['pval']=model_lm.pvalues
    opt_model=coeff[coeff['pval']<0.06]
    opt_model.reset_index(inplace=True)
    sd=None
    sd=[]
    for i in range(0,len(opt_model)):
        sd.append(opt_model['index'][i])
    opt_feature=pd.DataFrame(sd,columns=['opt_feature'])
    opt_feature=opt_feature[opt_feature['opt_feature']!='Intercept']
    opt_feature_=opt_feature.T
    feature_sel = opt_feature_[opt_feature_.columns[0:]].apply(lambda x: "','".join(x.dropna().astype(str)),axis=1)
    opt_feature_['final_opt_feature'] = opt_feature_[opt_feature_.columns[0:]].apply(lambda x: '+'.join(x.dropna().astype(str)),axis=1)
    opt_feature_.reset_index(inplace=True)
    model_lm2=smf.ols(formula="usage_ ~ "+opt_feature_['final_opt_feature'][0]+"", data=trainning_set_demo).fit()
    return model_lm2,feature_sel[0],best_parameter,model_lm
def Model_Evaluation_Testing(best_parameter,model_lm2,testing_set,trainning_set):
    #testing_set['period']=testing_set['period'].astype('category')
    #trainning_set['period']=trainning_set['period'].as type('category')
    r=np.arange(1,49)
    for i in range(1,len(r)):
        ki=str(i)
        trainning_set["period_"+ki+""]=np.where(trainning_set.period==i,1,0)  
    r=np.arange(1,49)
    for i in range(1,len(r)):
        ki=str(i)
        testing_set["period_"+ki+""]=np.where(testing_set.period==i,1,0)
    try:
        x_test=testing_set[['period_1','period_2','period_3','period_4','period_5','period_6','period_7','period_8','period_9','period_10','period_11','period_12','period_13','period_14','period_15','period_16','period_17','period_18','period_19','period_20','period_21','period_22','period_23','period_24','period_25','period_26','period_27','period_28','period_29','period_30','period_31','period_32','period_33','period_34','period_35','period_36','period_37','period_38','period_39','period_40','period_41','period_42','period_43','period_44','period_45','period_46','period_47','Mon','Tue','Wed','Thu','Fri','Sat','Is_holiday','lag1_usage','lag2_usage','lag3_usage','lag4_usage','lag5_usage','lag1_traffic','lag2_traffic','lag3_traffic','lag4_traffic','lag5_traffic','traffic_count']]
        y_test=testing_set[['usage']]
        x_train=trainning_set[['period_1','period_2','period_3','period_4','period_5','period_6','period_7','period_8','period_9','period_10','period_11','period_12','period_13','period_14','period_15','period_16','period_17','period_18','period_19','period_20','period_21','period_22','period_23','period_24','period_25','period_26','period_27','period_28','period_29','period_30','period_31','period_32','period_33','period_34','period_35','period_36','period_37','period_38','period_39','period_40','period_41','period_42','period_43','period_44','period_45','period_46','period_47','Mon','Tue','Wed','Thu','Fri','Sat','Is_holiday','lag1_usage','lag2_usage','lag3_usage','lag4_usage','lag5_usage','lag1_traffic','lag2_traffic','lag3_traffic','lag4_traffic','lag5_traffic','traffic_count']]
        y_train=trainning_set[['usage']]
    except:
        x_test=testing_set[['period_1','period_2','period_3','period_4','period_5','period_6','period_7','period_8','period_9','period_10','period_11','period_12','period_13','period_14','period_15','period_16','period_17','period_18','period_19','period_20','period_21','period_22','period_23','period_24','period_25','period_26','period_27','period_28','period_29','period_30','period_31','period_32','period_33','period_34','period_35','period_36','period_37','period_38','period_39','period_40','period_41','period_42','period_43','period_44','period_45','period_46','period_47','Mon','Tue','Wed','Thu','Fri','Sat','Is_holiday','lag1_usage','lag2_usage','lag3_usage','lag4_usage','lag5_usage']]
        y_test=testing_set[['usage']]
        x_train=trainning_set[['period_1','period_2','period_3','period_4','period_5','period_6','period_7','period_8','period_9','period_10','period_11','period_12','period_13','period_14','period_15','period_16','period_17','period_18','period_19','period_20','period_21','period_22','period_23','period_24','period_25','period_26','period_27','period_28','period_29','period_30','period_31','period_32','period_33','period_34','period_35','period_36','period_37','period_38','period_39','period_40','period_41','period_42','period_43','period_44','period_45','period_46','period_47','Mon','Tue','Wed','Thu','Fri','Sat','Is_holiday','lag1_usage','lag2_usage','lag3_usage','lag4_usage','lag5_usage']]
        y_train=trainning_set[['usage']]
    yhat=model_lm2.predict(x_test)
    yhat_train=model_lm2.predict(x_train)
    try:
        y_test['predicted']=yhat**(1/np.array(best_parameter.opt_parms))
    except:
        y_test['predicted']=yhat
    y_test['predicted']=np.where(y_test.predicted<0,0,y_test.predicted)
    y_test['predicted']=np.where(y_test.predicted>y_test.usage.max(),y_test.usage.max(),y_test.predicted)
    y_test['MAPE']=(np.abs((y_test.usage - y_test.predicted) / y_test.usage)).astype(float)
    y_test['MAPE']=y_test['MAPE'].astype(str)
    y_test['MAPE']=np.where(y_test.MAPE == 'inf',0,y_test.MAPE)
    y_test['MAPE']=np.where(y_test.MAPE == 'nan',0,y_test.MAPE)
    y_test['MAPE']=y_test['MAPE'].astype(float)
    Mape=np.mean(y_test['MAPE'])*100
    
    try:
        y_train['predicted']=yhat_train**(1/np.array(best_parameter.opt_parms))
    except:
        y_train['predicted']=yhat_train
    y_train['predicted']=np.where(y_train.predicted<0,0,y_train.predicted)
    y_train['predicted']=np.where(y_train.predicted>y_train.usage.max(),y_train.usage.max(),y_train.predicted)
    y_train['MAPE']=(np.abs((y_train.usage - y_train.predicted) / y_train.usage)).astype(float)
    y_train['MAPE']=y_train['MAPE'].astype(str)
    y_train['MAPE']=np.where(y_train.MAPE == 'inf',0,y_train.MAPE)
    y_train['MAPE']=np.where(y_train.MAPE == 'nan',0,y_train.MAPE)
    y_train['MAPE']=y_train['MAPE'].astype(float)
    Mape_train=np.mean(y_train['MAPE'])*100
    return Mape,Mape_train

def Model_Data_Formation_Traffic(train_set):
    train_set['Mon']=np.where(train_set.weekday==0, 1,0)
    train_set['Tue']=np.where(train_set.weekday==1,1,0)
    train_set['Wed']=np.where(train_set.weekday==2,1,0)
    train_set['Thu']=np.where(train_set.weekday==3,1,0)
    train_set['Fri']=np.where(train_set.weekday==4,1,0)
    train_set['Sat']=np.where(train_set.weekday==5,1,0)
    train_set['Sun']=np.where(train_set.weekday==6,1,0)
    train_set['Is_holiday']=np.where((train_set.weekday==5)| (train_set.weekday==6), 1,0)
    new=train_set['date_time'].str.split(" ",expand=True)
    train_set['date']=new[0]
    train_set['Is_holiday']=np.where((train_set['date']=='2019-05-27')|(train_set['date']=='2019-07-04')|(train_set['date']=='2019-07-05'),1,train_set['Is_holiday'])
    train_set['lag1_traffic'] = train_set['traffic_count'].shift(1)
    train_set['lag2_traffic'] = train_set['lag1_traffic'].shift(1)
    train_set['lag3_traffic'] = train_set['lag2_traffic'].shift(1)
    train_set['lag4_traffic'] = train_set['lag3_traffic'].shift(1)
    train_set['lag5_traffic'] = train_set['lag4_traffic'].shift(1)
    train_set=train_set.fillna(0)
    trainning_set=train_set[(train_set['date_time'] > '2019-05-01 00:00:00') & (train_set['date_time'] <= '2019-08-22 23:30:00')]
    testing_set=train_set[(train_set['date_time'] > '2019-08-23 00:00:00') & (train_set['date_time'] <= '2019-08-31 23:30:00')]
    return trainning_set,testing_set

def Model_Building_Traffic(trainning_set):
    r=np.arange(1,49)
    for i in range(1,len(r)):
        ki=str(i)
        trainning_set["period_"+ki+""]=np.where(trainning_set.period==i,1,0)  
    #trainning_set['period']=trainning_set['period'].astype('category')
    opt_parameter=[None,0.05,0.10,0.15,0.20,0.25,0.25,0.30,0.35,0.40,0.45,0.50,0.55,0.60,0.65,0.70,0.75,0.80,0.85,0.90,0.95]
    r_sq=[]
    op_par=[]
    for i in range(0,len(opt_parameter)):
        try:
            trainning_set['traffic_count_']=trainning_set['traffic_count']**opt_parameter[i]
        except:
            trainning_set['traffic_count_']=trainning_set['traffic_count']
        model_lm=smf.ols(formula='traffic_count_ ~ period_1+period_2+period_3+period_4+period_5+period_6+period_7+period_8+period_9+period_10+period_11+period_12+period_13+period_14+period_15+period_16+period_17+period_18+period_19+period_20+period_21+period_22+period_23+period_24+period_25+period_26+period_27+period_28+period_29+period_30+period_31+period_32+period_33+period_34+period_35+period_36+period_37+period_38+period_39+period_40+period_41+period_42+period_43+period_44+period_45+period_46+period_47+Mon+Tue+Wed+Thu+Fri+Sat+Is_holiday+lag1_traffic+lag2_traffic+lag3_traffic+lag4_traffic+lag5_traffic', data=trainning_set).fit()
        r_sq.append(model_lm.rsquared)
        op_par.append(opt_parameter[i])
    dm=pd.DataFrame(r_sq,columns=['r_squared'])
    dm['opt_parms']=op_par
    best_parameter=dm[dm.r_squared>=dm.r_squared.max()]
    try:
        trainning_set['traffic_count_']=trainning_set['traffic_count']**np.array(best_parameter.opt_parms)
    except:
        trainning_set['traffic_count_']=trainning_set['traffic_count'] 
    model_lm=smf.ols(formula='traffic_count_ ~ period_1+period_2+period_3+period_4+period_5+period_6+period_7+period_8+period_9+period_10+period_11+period_12+period_13+period_14+period_15+period_16+period_17+period_18+period_19+period_20+period_21+period_22+period_23+period_24+period_25+period_26+period_27+period_28+period_29+period_30+period_31+period_32+period_33+period_34+period_35+period_36+period_37+period_38+period_39+period_40+period_41+period_42+period_43+period_44+period_45+period_46+period_47+Mon+Tue+Wed+Thu+Fri+Sat+Is_holiday+lag1_traffic+lag2_traffic+lag3_traffic+lag4_traffic+lag5_traffic', data=trainning_set).fit()
    coeff=pd.DataFrame(model_lm.params,columns=['coef'])
    coeff['pval']=model_lm.pvalues
    opt_model=coeff[coeff['pval']<0.06]
    opt_model.reset_index(inplace=True)
    sd=None
    sd=[]
    for i in range(0,len(opt_model)):
        sd.append(opt_model['index'][i])
    opt_feature=pd.DataFrame(sd,columns=['opt_feature'])
    opt_feature=opt_feature[opt_feature['opt_feature']!='Intercept']
    opt_feature_=opt_feature.T
    feature_sel = opt_feature_[opt_feature_.columns[0:]].apply(lambda x: "','".join(x.dropna().astype(str)),axis=1)
    opt_feature_['final_opt_feature'] = opt_feature_[opt_feature_.columns[0:]].apply(lambda x: '+'.join(x.dropna().astype(str)),axis=1)
    opt_feature_.reset_index(inplace=True)
    model_lm2=smf.ols(formula="traffic_count_ ~ "+opt_feature_['final_opt_feature'][0]+"", data=trainning_set).fit()
    return model_lm2,feature_sel[0],best_parameter,model_lm

def Model_Evaluation_Testing_Traffic(best_parameter,model_lm2,testing_set,trainning_set):
    #testing_set['period']=testing_set['period'].astype('category')
    #trainning_set['period']=trainning_set['period'].as type('category')
    r=np.arange(1,49)
    for i in range(1,len(r)):
        ki=str(i)
        trainning_set["period_"+ki+""]=np.where(trainning_set.period==i,1,0)  
    r=np.arange(1,49)
    for i in range(1,len(r)):
        ki=str(i)
        testing_set["period_"+ki+""]=np.where(testing_set.period==i,1,0)
    x_test=testing_set[['period_1','period_2','period_3','period_4','period_5','period_6','period_7','period_8','period_9','period_10','period_11','period_12','period_13','period_14','period_15','period_16','period_17','period_18','period_19','period_20','period_21','period_22','period_23','period_24','period_25','period_26','period_27','period_28','period_29','period_30','period_31','period_32','period_33','period_34','period_35','period_36','period_37','period_38','period_39','period_40','period_41','period_42','period_43','period_44','period_45','period_46','period_47','Mon','Tue','Wed','Thu','Fri','Sat','Is_holiday','lag1_traffic','lag2_traffic','lag3_traffic','lag4_traffic','lag5_traffic']]
    y_test=testing_set[['traffic_count']]
    x_train=trainning_set[['period_1','period_2','period_3','period_4','period_5','period_6','period_7','period_8','period_9','period_10','period_11','period_12','period_13','period_14','period_15','period_16','period_17','period_18','period_19','period_20','period_21','period_22','period_23','period_24','period_25','period_26','period_27','period_28','period_29','period_30','period_31','period_32','period_33','period_34','period_35','period_36','period_37','period_38','period_39','period_40','period_41','period_42','period_43','period_44','period_45','period_46','period_47','Mon','Tue','Wed','Thu','Fri','Sat','Is_holiday','lag1_traffic','lag2_traffic','lag3_traffic','lag4_traffic','lag5_traffic']]
    y_train=trainning_set[['traffic_count']]
    yhat=model_lm2.predict(x_test)
    yhat_train=model_lm2.predict(x_train)
    try:
        y_test['predicted']=yhat**(1/np.array(best_parameter.opt_parms))
    except:
        y_test['predicted']=yhat
    y_test['predicted']=np.where(y_test.predicted<0,0,y_test.predicted)
    y_test['predicted']=np.where(y_test.predicted>y_test.traffic_count.max(),y_test.traffic_count.max(),y_test.predicted)
    y_test['MAPE']=(np.abs((y_test.traffic_count - y_test.predicted) / y_test.traffic_count)).astype(float)
    y_test['MAPE']=y_test['MAPE'].astype(str)
    y_test['MAPE']=np.where(y_test.MAPE == 'inf',0,y_test.MAPE)
    y_test['MAPE']=np.where(y_test.MAPE == 'nan',0,y_test.MAPE)
    y_test['MAPE']=y_test['MAPE'].astype(float)
    Mape=np.mean(y_test['MAPE'])*100
    
    try:
        y_train['predicted']=yhat_train**(1/np.array(best_parameter.opt_parms))
    except:
        y_train['predicted']=yhat_train
    y_train['predicted']=np.where(y_train.predicted<0,0,y_train.predicted)
    y_train['predicted']=np.where(y_train.predicted>y_train.traffic_count.max(),y_train.traffic_count.max(),y_train.predicted)
    y_train['MAPE']=(np.abs((y_train.traffic_count - y_train.predicted) / y_train.traffic_count)).astype(float)
    y_train['MAPE']=y_train['MAPE'].astype(str)
    y_train['MAPE']=np.where(y_train.MAPE == 'inf',0,y_train.MAPE)
    y_train['MAPE']=np.where(y_train.MAPE == 'nan',0,y_train.MAPE)
    y_train['MAPE']=y_train['MAPE'].astype(float)
    Mape_train=np.mean(y_train['MAPE'])*100
    return Mape,Mape_train