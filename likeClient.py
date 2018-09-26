import socket

from Crazy.ToolMethod import ComposingMsg, DecomposingLength, DecomposingMsg

dict={'key':'0012017-10-10T18:44:54Z2017-10-10T18:50:54Z'}
sock=socket.socket()
server_addr=('127.0.0.1',8001)
sock.connect(server_addr)
i=1
while i:
    #1.send 时间戳
    msg=ComposingMsg(dict)
    print(msg)
    sock.send(msg)
    #ret = str(sock.recv(1024),encoding="utf-8")
    print('send 成功')
    #2。接收数据流
    bytes_length=sock.recv(4)
    length=DecomposingLength(bytes_length)
    bytes_dataflow=sock.recv(length)
    dataflow=DecomposingMsg(bytes_dataflow)
    print(dataflow)

    i=i-1
sock.close()
