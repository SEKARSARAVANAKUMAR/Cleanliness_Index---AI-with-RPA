import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
url = "http://www.massport.com/logan-airport/flights/flight-status/"
#url = "http://www.massport.com/logan-airport/flights/flight-status/#Departures"
 
# Getting the webpage, creating a Response object.
response = requests.get(url)
 
# Extracting the source code of the page.
data = response.text
 
# Passing the source code to BeautifulSoup to create a BeautifulSoup object for it.
soup = BeautifulSoup(data, 'lxml')
 
# Extracting all the <a> tags into a list.
tags = soup.find_all('a')
 
# Extracting URLs from the attribute href in the <a> tags.
for tag in tags:
    href_tag=tag.get('href')
rk=soup.find_all('script',type="text/javascript")[5]
rk_=str(rk)


class BracketMatch:
    def __init__(self, refstr, parent=None, start=-1, end=-1):
        self.parent = parent
        self.start = start
        self.end = end
        self.refstr = refstr
        self.nested_matches = []
    def __str__(self):
        cur_index = self.start+1
        result = ""
        if self.start == -1 or self.end == -1:
            return ""
        for child_match in self.nested_matches:
            if child_match.start != -1 and child_match.end != -1:
                result += self.refstr[cur_index:child_match.start]
                cur_index = child_match.end + 1
            else:
                continue
        result += self.refstr[cur_index:self.end]
        return result

# Main script
#haystack = '''[ this is [ hello [ who ] [what ] from the other side ] slim shady ]'''
root = BracketMatch(rk_)
cur_match = root
for i in range(len(rk_)):
    if '[' == rk_[i]:
        new_match = BracketMatch(rk_, cur_match, i)
        cur_match.nested_matches.append(new_match)
        cur_match = new_match
    elif ']' == rk_[i]:
        cur_match.end = i
        cur_match = cur_match.parent
    else:
        continue
# Here we built the set of matches, now we must print them
nodes_list = root.nested_matches
# So we conduct a BFS to visit and print each match...
while nodes_list != []:
    node = nodes_list.pop(0)
    nodes_list.extend(node.nested_matches)
    #print("Match: " + str(node).strip())
final_json=str(node).strip()
class BracketMatch:
    def __init__(self, refstr, parent=None, start=-1, end=-1):
        self.parent = parent
        self.start = start
        self.end = end
        self.refstr = refstr
        self.nested_matches = []
    def __str__(self):
        cur_index = self.start+1
        result = ""
        if self.start == -1 or self.end == -1:
            return ""
        for child_match in self.nested_matches:
            if child_match.start != -1 and child_match.end != -1:
                result += self.refstr[cur_index:child_match.start]
                cur_index = child_match.end + 1
            else:
                continue
        result += self.refstr[cur_index:self.end]
        return result

# Main script
#haystack = '''[ this is [ hello [ who ] [what ] from the other side ] slim shady ]'''
root = BracketMatch(final_json)
cur_match = root
for i in range(len(final_json)):
    if '{' == final_json[i]:
        new_match = BracketMatch(final_json, cur_match, i)
        cur_match.nested_matches.append(new_match)
        cur_match = new_match
    elif "}" == final_json[i]:
        cur_match.end = i
        cur_match = cur_match.parent
    else:
        continue
# Here we built the set of matches, now we must print them
nodes_list = root.nested_matches
# So we conduct a BFS to visit and print each match...
cnt=0
while nodes_list != []:
    node = nodes_list.pop(0)
    nodes_list.extend(node.nested_matches)
    #print("Match: " + str(node).strip())
    final_json__=str(node).strip()
    df = pd.DataFrame([x.split(',') for x in final_json__.split('\n')])
    key=[]
    value=[]
    cnt=cnt+1
    for j in range(0,df.shape[1]):
        if j==0:
            key.append(df[j].str.split(':')[0][0])
            value.append(df[j].str.split(':')[0][2])
        elif (j==8) | (j==18) |(j==23) |(j==24):
            key.append(df[j].str.split(':')[0][0])
            try:
                value.append(df[j].str.split('":"')[0][1])
            except:
                value.append(None)
        else:
            key.append(df[j].str.split(':')[0][0])
            try:
                value.append(df[j].str.split(':')[0][1])
            except:
                value.append(None)
    new_frame=pd.DataFrame(key,columns=['key'])
    new_frame['value']=value
    new_frame.set_index(['key'],inplace=True)
    new_frame_ =new_frame.T
    new_frame_.reset_index(inplace=True)
    if cnt==1:
        Flight_data=new_frame_
    else:
        n2=new_frame_
        Flight_data=pd.concat([Flight_data,n2],axis=0)
        Flight_data.reset_index(inplace=True)
        Flight_data.drop(['index','level_0'],axis=1,inplace=True)
print(Flight_data.head(10))