import socket
import bson



server_addr=('127.0.0.1',8787)
client=socket.socket()
client.connect(server_addr)

local={
    'hash001':'eric',
    'hash002':'lily',
    'hash003':'bonnie'
}

dataflow = {
    'flag':'d',
    'Client_IP':'127.0.0.1',
    'Client_Port': 8787,
    'num': 0
}

req2 = {
    'flag':'p',
    'Client_IP': '127.0.0.1',
    'Client_Port':8787,
    'num': 3,
    'hashList': ['hash001', 'hash002', 'hash003']
}
bson_req2=bson.dumps(req2)

#1.发送req2
le = len(bson_req2)
msg = bytes(str(le).zfill(4), encoding='utf-8') + bson_req2
client.send(msg)
#2.接收rest_req2,得出dataflow，寄出
bytes_rest_msg=client.recv(1024)
bytes_rest_data=bytes_rest_msg[4:]
rest_data=bson.loads(bytes_rest_data)
hashList=rest_data['hashList']
for item in hashList:
    if local[item]:
        dataflow[item]=local[item]
        dataflow['num'] +=1
print('121',dataflow)
bson_dataflow=bson.dumps(dataflow)
length = len(bson_dataflow)
print(length)
msg_msg = bytes(str(length).zfill(4), encoding='utf-8') + bson_dataflow
print(msg_msg)
client.send(msg_msg)
print(msg_msg,'is send successfully')
client.close()


