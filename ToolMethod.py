import bson

#功能：将一个{}报文转成bson加上其四位长度组装成一个二进制数据报
def ComposingMsg(dict):
    bson_data=bson.dumps(dict)
    length=len(bson_data)
    str_length=str(length).zfill(4)
    bytes_Msg=bytes(str_length,encoding='utf-8')+bson_data
    return bytes_Msg
#接受4字节长度数据，解析成他的接受包的长度
def DecomposingLength(bytes_length):
    str_length=bytes_length.decode()
    length=int(str_length)
    print('length is:',length)
    return length

#功能：将接收到的bytes数据解包，解析成dict并返回
def DecomposingMsg(bytes_Msg):
    bson_data=bson.loads(bytes_Msg)
    #这个地方数据报长度没有利用到，如果需要在这里取
    return bson_data

#将起始时间组装成需要的报文格式
def ComposingClientRecord(dict,sk):
    #'0012017-10-10T18:44:54Z2017-10-10T18:50:54Z'
    #将他组成：
    '''
                   clientRecord = {
                    'Client_IP': '127.0.0.1',
                    'Client_Port': '10000',
                    'Time_s': '2017-10-10T18:44:54Z',
                    'Time_e': '2017-10-10T18:50:54Z',
                    'flag': 'p',
                    'SensorID': 0
                }
    '''
    msg=dict['key']
    ClientRecord={}
    ClientRecord['Client_IP']=sk.getpeername()[0]
    ClientRecord['Client_Port'] = sk.getpeername()[1]
    ClientRecord['sensorID'] = msg[0:3]
    ClientRecord['Time_s'] = msg[3:23]
    ClientRecord['Time_e'] = msg[23:]
    ClientRecord['flag'] = 'p'
    return ClientRecord



