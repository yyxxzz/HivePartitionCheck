# -*- coding: utf-8 -*-
from snakebite.minicluster import MiniCluster
from snakebite.client import Client
import pprint
import smtplib
from email.mime.text import MIMEText
import csv

'''
配置参数
'''
#Hive数据仓库路径
Hive_Warehouse='/data/yoho/user/'
client = Client('yhbd01',8020,use_trash=False)
#发送邮件配置信息
mailto_list=['jinyuan.du@yoho.cn','liming.hu@yoho.cn','longhuan.zhang@yoho.cn'] 
mail_host="smail.yoho.cn"  #设置服务器
mail_user="jinyuan.du"    #用户名
mail_pass="rorovic33932007262"   #口令 
mail_postfix="yoho.cn"  #发件箱的后缀

'''
由Hive库的路径,获取DB的路径
'''
def get_DB(Hive_Warehouse):
    DB={}
    DB_re=[]
    client = Client('yhbd01',8020,use_trash=False)
    list_hive = list(client.ls([Hive_Warehouse],include_toplevel=False, include_children=True,recurse=True))
    for x in list_hive:
        DB=x
        DB_re.append(DB['path'])
    #print 'hive表路径扫描完毕！'
    return DB_re

'''
根据路径获取，路径信息
'''
def get_Count():
    list_part = get_DB(Hive_Warehouse)
    list_status=[]
    for i in list_part:
        
        list_status.append(list(client.count([i])))
  
    #print '根据路径获取信息！'
    return list_status
'''
将路径length为0的路径过滤出来
'''
def get_result():
    csvfile = open("hive_result.csv",'wb')
    csvWriter = csv.writer(csvfile)
    #title_text=['Hive表分区异常信息：分区存在，但是没有数据！分区列表如下！']
    titlelist=['path','length']
    csvWriter.writerow(titlelist)

    list_status = get_Count()
    for i in range(len(list_status)):
        if list_status[i][0]['length'] == 0 and '_DONE' not in list_status[i][0]['path'] and '0000' not in list_status[i][0]['path'] and '0001' not in list_status[i][0]['path']:
            #print '-----------------'
            #pprint.pprint(list_status[i][0]['path'])
            #pprint.pprint(list_status[i][0]['length'])
            path_result = "表分区路径："+list_status[i][0]['path']
            length_result = "分区length："+str(list_status[i][0]['length'])
            csv_list = [path_result,length_result]
            csvWriter.writerow(csv_list)
            #print '-----------------'
    csvfile.close()
    #print '筛选出已有分区无数据的路径！Done！'

'''
发送邮件方法
'''
def send_email(to_list,sub,content):
    me="hello"+"<"+mail_user+"@"+mail_postfix+">"  
    msg = MIMEText(content,_subtype='plain',_charset='gb2312')  
    msg['Subject'] = sub  
    msg['From'] = me  
    msg['To'] = ";".join(to_list)  
    try:  
        server = smtplib.SMTP()  
        server.connect(mail_host)  
        server.login(mail_user,mail_pass)  
        server.sendmail(me, to_list, msg.as_string())  
        server.close()  
        return True  
    except Exception, e:  
        print str(e)  
        return False  
    
'''
读取生成文件，并发送邮件
'''
def send_action():
    content_list = []
    csvReader = csv.reader(open('hive_result.csv','rb'))     
    for row in csvReader:
        line = ','.join(row)
        content_list.append(line+"\n")
    if send_email(mailto_list,"Hive表分区异常信息：分区存在，但是没有数据！分区列表如下！",''.join(content_list)):
        print "发送成功"
    else:
        print "发送失败"

if __name__ == '__main__':
   get_result()
   send_action()  

