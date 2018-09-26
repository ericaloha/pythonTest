# 写一个轮询的server模型：
# 1.接收client发来的时间戳数据，转接给mongo，换句话说本地存一下，面向Mongo时cache是client
# 2.发数据给client，这个更单纯，直接从dataflowlist里找，找到对应clientid的直接输出
import select
import socket
from Crazy.ToolMethod import DecomposingMsg, ComposingClientRecord, DecomposingLength, ComposingMsg

ClientRecordList = []
dataflow = {
    'Client_IP': '127.0.0.1',
    'Client_Port': 8001,
    'hash001': 'lili',  # over代表最后一个数据流
    'hash002': 'bonnie'
}
dataflowList=[dataflow,]
sk1 = socket.socket()
sk1.bind(("101.76.212.104", 8001))
sk1.listen()
print('waiting for client。。。。')

inputs = [sk1, ]

while True:
    r_list, w_list, e_list = select.select(inputs, [], [], 1)
    for sk in r_list:
        if sk == sk1:
            conn, address = sk.accept()
            print(conn, 'is linked to cache')
            inputs.append(conn)
        else:
            try:
                '''
                # 1.接收字符串的时间戳和sensorID，并存储在一个全局容器内以便clientforMongo调取
                #bytes_length = sk.recv(4)
                #length = DecomposingLength(bytes_length)
                #bytes_dict = sk.recv(length)
                # '0012017-10-10T18:44:54Z2017-10-10T18:50:54Z'
                #dict = DecomposingMsg(bytes_dict)
                #msg = ComposingClientRecord(dict, sk)
                # 这个msg其实就是两个时间戳，起始时间和结束时间，要组合成下面这个样子
                #print(21111, msg)
                #ClientRecordList.append(msg)
                #print(ClientRecordList)

                # 2.从dataflowList里面找clientIP一致的dataflow传给client就好了
                for item in dataflowList:
                    if item['Client_IP'] == sk.getpeername()[0]:
                        sk.send(ComposingMsg(item))
                        print('dataflow is send successfully!!!')
                '''
                recv_data=sk.recv(1024).decode()
                print('1221',recv_data)
                i=0
                timestamp='2017-10-10T18:44:54Z'
                while i<10:
                    sk.send(timestamp.encode())
                    i=i+1
                    print('send successfully')
                flag='Finish'
                sk.send(flag.encode())
                print('send over...      ')
            except Exception as ex:
                inputs.remove(sk)

