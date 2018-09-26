import socket

from Crazy.ToolMethod import decomposingMsg, composingMsg, decomposingLength
import socket

redisRecord = {
    'Client_IP': '127.0.0.1',
    'Client_Port': '10000',
    'num': 3,
    'hashList': ['hash001', 'hash002', 'hash003']
}
dataflow = {
    'Client_IP': '127.0.0.1',
    'Client_Port': 10000,
    'Timestamp':'2017-10-10T18:44:54Z',
    'Key': 'hash001',
    'v_data': bytes('lalaal',encoding='utf-8'),  # over代表最后一个数据流
    'num_th': 2  # 0代表最后一个数据流
}

dataflow_over = {
    'Client_IP': '127.0.0.1',
    'Client_Port': 10000,
    'Timestamp':'2017-10-10T18:44:54Z',
    'Key': 'hash001',
    'v_data': bytes('dididafed',encoding='utf-8'),  # over代表最后一个数据流
    'num_th': 0  # 0代表最后一个数据流
}
s = socket.socket()
s.bind(('127.0.0.1', 8787))
s.listen(5)
print('waiting for client')
while True:
    c, addr = s.accept()
    print('server is here:', c)
    bytes_length = c.recv(4)
    length = decomposingLength(bytes_length)
    bytes_req2 = c.recv(length)
    req2 = decomposingMsg(bytes_req2)
    print('成功收到clientRecord:', req2)
    i=0
    while i<100:
        dataflow['v_data']=bytes('lalaal',encoding='utf-8')
        bytes_dataflow = composingMsg(dataflow)
        c.send(bytes_dataflow)
        print('send successfully!')
        i+=1
    c.send(composingMsg(dataflow_over))
    print('send over.....')
    c.close()
