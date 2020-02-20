#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pymongo import MongoClient
import bs4 as bs
import requests
import re
import pandas as pd
import time
import json
import sys
import datetime
import csv

#connect to MongoDB using pymongo
client = MongoClient('localhost', 27017)
db = client['form4']
collection = db['form4_collection']

#requests + beautifulSoup function
def f_soup(url): 
    #spoof user agent 
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    while True:
        try:
            request = requests.get(url, headers=headers)
        except Exception as error: #try again if request fails
            print("## ERROR ##: ", url, error)
            time.sleep(10)
            continue
        break
    
    #convert to .text so beautifulSoup can read data
    #run text through beautifulSoup 
    response = request.text 
    soup = bs.BeautifulSoup(response, 'xml') 
    return soup

#append "https://www.sec.gov" to URLs    
def append_sec(partial_url_list):
    full_url_list = []
    for x in partial_url_list:
        if x.has_attr('href'): 
            url = x['href']
            full_url_list.append("https://www.sec.gov" + url)        
    return full_url_list

#add key, values of Form 4 XML to form4_dict{}
def f_dictionary(form4_soup): 
    form4_dict = {}
    form4_key_list =['documentType', 'periodOfReport', 'issuerName', 'issuerTradingSymbol',
                      'issuerCik', 'rptOwnerCik','rptOwnerName','transactionCode']
    for key in form4_key_list:  
        find_key = form4_soup.find(key) 
        if find_key: #if the the key is found
            value = find_key.text 
            if key == 'periodOfReport': 
                value = value[:10] #remove trailing characters from date
            else:
                pass       
        else: #if the key is not found
            value = "N/A"
        form4_dict.update({key : value})    

    #parents of 'value' tags must be used for key in form4_dict{}
    soup_value = form4_soup.find_all('value')
    for tag in soup_value:
        key = tag.parent.name
        value = tag.text
        form4_dict.update({key : value})
    return form4_dict

#format dates for use with '>', '<' operators 
def format_date(date_to_format):  
    date_to_format = datetime.datetime.strptime(date_to_format, '%Y-%m-%d').date() 
    date_to_format = date_to_format.strftime('%Y-%m-%d')
    return date_to_format

#iterate over cik numbers in csv
def cik_loop(): 
    with open('path_to_csv' , newline= '') as csv_file:
        cik_reader = csv.reader(csv_file)
        for cik in cik_reader:
            cik = ''.join(cik) #convert from list to string
            main(cik)


def main(cik):  
    #page_number of URL
    page_number = 0 
    while True: 
        url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=' + cik + '&type=&dateb=&owner=only&start=' + str(page_number) + '&count=100'
        print(url)
        page_1_soup = f_soup(url) 
        partial_url_list = page_1_soup.find_all("a", id="documentsbutton") #parse for URL info
        page_2_url_list = append_sec(partial_url_list) #append "https://www.sec.gov" to partial_url_list items
        if len(page_2_url_list) == 0:      
            return #If 0 form4 URLs in list, go to next CIK         
        else:
            pass
        
        #return soup for each URL
        #find tables ('tr') on page which contain URLs
        find_table_list=[]
        for url in page_2_url_list: 
            page_2_soup = f_soup(url) 
            find_table = page_2_soup.find_all('tr') 
            find_table_list.extend(find_table)
        
        #append "https://www.sec.gov" to .xml url
        form4_partial_url_list = []
        for table in find_table_list:
            form4_partial_url = table.findAll('a', href=True, text= re.compile(r"\.xml$")) #find url that ends in .xml
            form4_partial_url_list.extend(form4_partial_url) 
        form4_xml_url_list = append_sec(form4_partial_url_list)
        if len(form4_xml_url_list) == 0:              
            return #if no .xml URLs in list, go to next CIK
        else:
            pass
        
        #beautifulSoup for form 4 .xml URLs 
        form4_dict_list = []
        for url in form4_xml_url_list:
            form4_soup = f_soup(url)            
            form_type = form4_soup.find('documentType') #'documentType' is form type
            form_type = form_type.text
            if form_type == '4' or form_type == '4/A':
                pass
            else:
                continue #skip form if not form 4
            
            #create a dictionary from form4 xml data  
            form4_dict = f_dictionary(form4_soup)     
            form4_dict_list.append(form4_dict) 
            
            #'periodOfReport' is filing date on form
            #forms prior to 'cut_off_date' will not be added to database
            for dictionary in form4_dict_list:
                date_on_form = format_date(dictionary['periodOfReport']) 
                cut_off_date = format_date('2009-01-01') 
                if date_on_form > cut_off_date:
                    pass 
                else:
                    return       
                     
            #write form4_dict to csv
            #Can also output to mongoDB here
            with open('test.csv', 'a') as final_csv: 
                for key in dictionary:
                    final_csv.write("%s,%s\n"%(key,dictionary[key]))
                    
#         print(len(form4_dict_list), 'Form 4 documents added to CSV')
        page_number = page_number + 100

cik_loop()                    


# In[ ]:





# In[ ]:




