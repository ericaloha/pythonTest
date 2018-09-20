from time import sleep

from Crazy.ToolMethod import decomposingMsg, composingMsg
import socket
redisRecord = {
    'Client_IP': '127.0.0.1',
    'Client_Port': '10000',
    'num': 3,
    'hashList': ['hash001', 'hash002', 'hash003']
}
s = socket.socket()
s.bind(('127.0.0.1',8787))
s.listen(5)
print('waiting for client')
dataflow1 = {
    'Client_IP': '127.0.0.1',
    'Client_Port': 8787,
    'Key': 'hash001',
    'Timestamp':'2017-10-10T18:44:54Z',
    'v_data': '1234',  # over代表最后一个数据流
    'num_th': 1  # 0代表最后一个数据流
}
dataflow2 = {
    'Client_IP': '127.0.0.1',
    'Client_Port': 8787,
    'Key': 'hash001',
    'Timestamp':'2017-10-12T18:12:54Z',
    'v_data': '43232',  # over代表最后一个数据流
    'num_th': 2  # 0代表最后一个数据流
}
dataflow3 = {
    'Client_IP': '127.0.0.1',
    'Client_Port': 8787,
    'Key': 'hash001',
    'v_data': '43232',  # over代表最后一个数据流
    'num_th': 0  # 0代表最后一个数据流
}
redisRecord = {
    'Client_IP': '127.0.0.1',
    'Client_Port': '10000',
    'num': 3,
    'hashList': ['hash001', 'hash002', 'hash003']
}
while True:
    c, addr = s.accept()
    print('server is here:',c)
    #1.第一个socket时间戳和hash
    bytes_clientRecord = c.recv(1024)
    clientRecord = decomposingMsg(bytes_clientRecord)
    print('成功收到clientRecord:', clientRecord)
    bytes_redisRecord = composingMsg(redisRecord)
    c.send(bytes_redisRecord)
    print('send successfully!')

    #2.第二个socket rest_req2和发流

    bytes_rest_req2 = c.recv(1024)
    print(bytes_rest_req2)
    rest_req2 = decomposingMsg(bytes_rest_req2)
    print('成功收到clientRecord:', rest_req2)
    
    bytes_dataflow1 = composingMsg(dataflow1)
    c.send(bytes_dataflow1)
    sleep(1)

    bytes_dataflow2 = composingMsg(dataflow2)
    c.send(bytes_dataflow2)
    sleep(1)

    bytes_dataflow3 = composingMsg(dataflow3)
    c.send(bytes_dataflow3)
    sleep(1)
    
    bytes_dataflow4 = composingMsg(dataflow2)
    c.send(bytes_dataflow4)
    sleep(1)

    bytes_dataflow5 = composingMsg(dataflow1)
    c.send(bytes_dataflow5)
    sleep(1)

    bytes_dataflow6 = composingMsg(dataflow3)
    c.send(bytes_dataflow6)
    sleep(1)


    print('send successfully!')
    c.close()
