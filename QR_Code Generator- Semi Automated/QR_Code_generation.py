from PIL import Image
import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import math
from datetime import datetime
date_time=datetime.utcnow()
sd=date_time.strftime('%Y-%m-%d %H:%M:%S')
mac_id=pd.read_excel("C:\\Users\\saravanan\\Desktop\\mac_id.xlsx")
mac_id.columns=['mac_id']
columns = 8
rows = 10
pages=math.ceil(len(mac_id)/80)

# ax enables access to manipulate each of subplots
for j in range(1,pages+1):
    if j==1:
        print(j)
        jj=str(j)
        ax=[]
        fig = plt.figure(figsize=(20,20))
        for i in range(columns*rows):
            try:
                url="https://chart.googleapis.com/chart?chs=150x150&cht=qr&chl=%22"+mac_id['mac_id'].ix[i]+"%22"
                img = Image.open(requests.get(url, stream=True).raw)
                ax.append( fig.add_subplot(rows, columns, i+1) )
                ax[-1].set_ylabel(""+mac_id['mac_id'].ix[i]+"")
                plt.xticks([])
                plt.yticks([])
                plt.imshow(img)
            except:
                print("limit Exceed (or) Data Gap")
        plt.savefig("C:\\Users\\saravanan\\Desktop\\QR_Code_"+jj+"_"+sd+".png")
        #plt.show()
    if j==2:
        print(j)
        jj=str(j)
        ax=[]
        fig = plt.figure(figsize=(20,20))
        for i in range(columns*rows):
            try:
                url="https://chart.googleapis.com/chart?chs=150x150&cht=qr&chl=%22"+mac_id['mac_id'].ix[i+80]+"%22"
                img = Image.open(requests.get(url, stream=True).raw)
                ax.append( fig.add_subplot(rows, columns, i+1) )
                ax[-1].set_ylabel(""+mac_id['mac_id'].ix[i+80]+"")
                plt.xticks([])
                plt.yticks([])
                plt.imshow(img)
            except:
                print("limit Exceed (or) Data Gap")
        plt.savefig("C:\\Users\\saravanan\\Desktop\\QR_Code_"+jj+"_"+sd+".png")
        #plt.show()
    if j==3:
        print(j)
        jj=str(j)
        ax=[]
        fig = plt.figure(figsize=(20,20))
        for i in range(columns*rows):
            try:
                url="https://chart.googleapis.com/chart?chs=150x150&cht=qr&chl=%22"+mac_id['mac_id'].ix[i+160]+"%22"
                img = Image.open(requests.get(url, stream=True).raw)
                ax.append( fig.add_subplot(rows, columns, i+1) )
                ax[-1].set_ylabel(""+mac_id['mac_id'].ix[i+160]+"")
                plt.xticks([])
                plt.yticks([])
                plt.imshow(img)
            except:
                print("limit Exceed (or) Data Gap")
        plt.savefig("C:\\Users\\saravanan\\Desktop\\QR_Code_"+jj+"_"+sd+".png")
        #plt.show()
    if j==4:
        print(j)
        jj=str(j)
        ax=[]
        fig = plt.figure(figsize=(20,20))
        for i in range(columns*rows):
            try:
                url="https://chart.googleapis.com/chart?chs=150x150&cht=qr&chl=%22"+mac_id['mac_id'].ix[i+240]+"%22"
                img = Image.open(requests.get(url, stream=True).raw)
                ax.append( fig.add_subplot(rows, columns, i+1) )
                ax[-1].set_ylabel(""+mac_id['mac_id'].ix[i+240]+"")
                plt.xticks([])
                plt.yticks([])
                plt.imshow(img)
            except:
                print("limit Exceed (or) Data Gap")
        plt.savefig("C:\\Users\\saravanan\\Desktop\\QR_Code_"+jj+"_"+sd+".png")
        #plt.show()
    if j==5:
        print(j)
        jj=str(j)
        ax=[]
        fig = plt.figure(figsize=(20,20))
        for i in range(columns*rows):
            try:
                url="https://chart.googleapis.com/chart?chs=150x150&cht=qr&chl=%22"+mac_id['mac_id'].ix[i+320]+"%22"
                img = Image.open(requests.get(url, stream=True).raw)
                ax.append( fig.add_subplot(rows, columns, i+1) )
                ax[-1].set_ylabel(""+mac_id['mac_id'].ix[i+320]+"")
                plt.xticks([])
                plt.yticks([])
                plt.imshow(img)
            except:
                print("limit Exceed (or) Data Gap")
        plt.savefig("C:\\Users\\saravanan\\Desktop\\QR_Code_"+jj+"_"+sd+".png")
        plt.show()
