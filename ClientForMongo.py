# 本地存储数据的数据结构
## 存client的地址和对应需要的时间戳和对应的哈希
from time import sleep
import socket
import redis
from Crazy.ToolMethod import composingMsg, decomposingMsg

# 1.这个类：处理cache和Mongo的关系
class CacheClient:

    # 初始化时传入要发送给mongo的时间戳数据
    def __init__(self, clientRecord):
        self.clientRecord = clientRecord

    ## 1.先架面向Mongo的client
    # a.socket信息
    sock = socket.socket()
    mongo_addr = ('101.76.216.146', 5000)

    # b.redis连接信息
    pool = redis.ConnectionPool(host='localhost', port=6379, password=123456)
    Redisconn = redis.Redis(connection_pool=pool)

    # c.client从本地时间戳缓存队列里取时间戳，将其发送给Mongo,并接收返回的哈希
    # 一次只发一个时间戳，发完就关掉socket
    def TransTimeStampToMongo(self):
        # 1.处理报文
        bson_Msg = composingMsg(self.clientRecord)
        # 2.连接mongo
        self.sock.connect(self.mongo_addr)
        # 3.发送给Mongo
        self.sock.send(bson_Msg)
        # 4.等待接收mongo的回复
        #sleep(1)
        bytes_Msg = self.sock.recv(1024)
        if bytes_Msg:
            Msg = decomposingMsg(bytes_Msg)
        # 5.socket工作完成，关闭socket
        self.sock.close()
        # 6.返回从mongo得到的报文（dict）
        if Msg:
            return Msg
        else:
            return 0


    # d.把上面结果来的hash进行处理，redis有的加到给client的队列，没有的形成报文返回
    def HandleHashWithRedis(self, new_req2, dataflowList):  # 这个req2就是前面一个方法的输出，这个报文已经被处理好了，也即是说输入的就是一个dict
        # 1.先处理数据，在本地redis进行查询，有的加入输出给client的队列，没有的形成报文返回给Mongo
        # 明确一个问题：接收数据的时候是一段一段的接收，但是最后总是要组成一个整体的视频记录，
        # 但是在redis存的时候不是一段一段的，是一个整体
        req2 = new_req2
        dataflow = {}
        dataflow['Client_IP'] = req2['Client_IP']
        dataflow['Client_Port'] = req2['Client_Port']
        hashList = req2['hashList']
        rest_hashList = []
        count = 0
        i = 0
        for item in hashList:
            if self.Redisconn.exists(item):
                vedio = self.Redisconn.get(item)
                dataflow[item] = vedio
                i += 1
            else:
                rest_hashList.append(item)
                count += 1
                dataflow['num'] = i
        dataflowList.append(dataflow)

        req2['hashList'] = rest_hashList
        req2['num'] = count
        return req2

    # e.把没有的hash报文发回给mongo，同时分段接收mongo返回的报文
    def TransHashToMongo(self, rest_req2,dataflowList):
        # 1.处理报文
        bytes_rest_req2 = composingMsg(rest_req2)
        # 2.开启socket
        self.sock.connect(self.mongo_addr)
        # 3.发送报文给Mongo
        self.sock.send(bytes_rest_req2)
        sleep(1)
        # 4.循环接收的过程
        ##首先定义接收用的的数据队列
        num = rest_req2['num']
        dataflow={}
        dataflow['Client_IP']=rest_req2['Client_IP']
        dataflow['Client_Port'] = rest_req2['Client_Port']
        ##然后定义接收的循环体
        while num:
            part_vedio = ''
            hash=rest_req2['hashList'][num-1]
            # 这是接收一个hash对应的几段数据
            bytes_data = self.sock.recv(2048)
            data = decomposingMsg(bytes_data)
            '''dataflow = {
                'Client_IP': '127.0.0.1',
                'Client_Port': 10000,
                'Key': 'hash001',
                'v_data': '...............',  # over代表最后一个数据流
                'num_th': 2  # 0代表最后一个数据流
            }
            '''
            dataflow['Timestamp'] = data['Timestamp']
            while not data['num_th'] == 0:
                part_vedio=part_vedio+data['v_data']
                bytes_data=self.sock.recv(2048)
                data=decomposingMsg(bytes_data)
            dataflow[hash]=part_vedio
            num -= 1
        dataflowList.append(dataflow)
        self.sock.close()
if __name__ == '__main__':

    ##这个玩意整合到控制台里
    # 1.第一部分，传时间戳
    # 要用一个数据结构存时间戳数据
    clientRecord = {
        'Client_IP': '127.0.0.1',
        'Client_Port': '10000',
        'Time_s': '2017-10-10T18:44:54Z',
        'Time_e': '2017-10-10T18:50:54Z',
        'flag': 'p',
        'SensorID': 0
    }
    cc = CacheClient(clientRecord)
    data = cc.TransTimeStampToMongo()
    print('recvd',data)

    # 2.第二部分 处理hash和redis数据
    # 要存dataflowList
    dataflowList = []
    #跟Mongo通信完返回的req2是这样的
    '''
    data={
        'Client_IP':'127.0.0.1',
        'Client_Port':10000,
        'flag':'d',
        'num':3,
        'hashList':['hash001','hash002','hash003']
    }

    dataflow={
        'Client_IP':'127.0.0.1',
        'Client_Port':10000,
        'Key':'hash001',
        'v_data':'...............',#over代表最后一个数据流
        'num_th':2  #0代表最后一个数据流
    }
    '''
    rest_req2 = cc.HandleHashWithRedis(data, dataflowList)
    print('will be sent', rest_req2)
    print('redis has:', dataflowList)

    #3.分块接收，整体存储视频数据
    cc.TransHashToMongo(rest_req2,dataflowList)
    print(dataflowList)

