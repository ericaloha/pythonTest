import select
import socket
from time import sleep

import bson
import redis
#1.socket信息


server=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
server.setblocking(0)
server_addr=('127.0.0.1',8787)
#提示信息
print('Cache is starting up on',server_addr)
server.bind(server_addr)
server.listen(5)

#2.自定义函数
def handleReq2withRedis(dataflowList,redisRecord):
    ##定义redis连接
    pool = redis.ConnectionPool(host='localhost', port=6379, password=123456)
    Redisconn = redis.Redis(connection_pool=pool)
    new_hashlist=[]
    dataflow={}
    '''
    dataflow={
       'client_addr':('127.0.0.1',10000),
        'mongo_addr':('127.0.0.1',10001),

        'num':2,
        'hash001':'eric',
        'hash002':'bonnie'
    }
    dataFlowList=[dataflow1,dataflow2]
    '''
    ##处理过程
    Record_Buffer=redisRecord
    num=Record_Buffer['num']
    hashList=Record_Buffer['hashList']
    i=0
    count=0
    NewNum=0
    while i<num:
        if Redisconn.exists(hashList[i]):
            dataflow[hashList[i]]=Redisconn.get(hashList[i])
            count=count+1
        else:
            new_hashlist.append(hashList[i])
            NewNum = NewNum + 1
        i=i+1
    #处理dataflowDict
    dataflow['num']=count
    dataflow['Client_IP']=Record_Buffer['Client_IP']
    dataflow['Client_Port'] = Record_Buffer['Client_Port']
    dataflowList.append(dataflow)
    print('now this is the dataflowlist from func:',dataflowList)

    #处理redisRecord
    Record_Buffer['num']=NewNum
    Record_Buffer['hashList']=new_hashlist
    return Record_Buffer

#3.本地缓存的数据结构
dataflowList=[]

#4.轮询过程
inputs=[server]

while True:
    r_list,w_list,e_list=select.select(inputs,[],[],1)
    #print('正在监听{0}个数据'.format(len(inputs)))
    for sk in r_list:
        if sk == server:
            #表示有用户来连接server了
            conn,addr=sk.accept()
            inputs.append(conn)
            print(conn,'is connect to cache')
        else:#老用户，接收数据
            try:
                bytes_data=sk.recv(1024)
                print('nono',bytes_data)
                bson_data = bytes_data[4:]
                print('didi',bson_data)
                data=bson.loads(bson_data)
                print(data)
                if data['flag']=='p':#证明是req2
                    #(1)接收req2并把本地缺失的返还给Mongo
                    rest_req2 = handleReq2withRedis(dataflowList, data)
                    print('12121',dataflowList)
                    print('2121',rest_req2)
                    rest_bson=bson.dumps(rest_req2)
                    len=len(rest_bson)

                    rest_msg=bytes(str(len).zfill(4),encoding='utf-8')+rest_bson
                    print("hello:",rest_msg)
                    sk.send(rest_msg)
                    sleep(1)
                    bytes_flow=sk.recv(1024)
                    print('wer',bytes_flow)
                    bytes_dataflow=bytes_flow[4:]
                    df=bson.loads(bytes_dataflow)
                    print('bonnie',df)
            except:#空消息表示连接中断，所以要在监听中移除
                print(sk.getsockname(),'will be removed')
                inputs.remove(sk)

