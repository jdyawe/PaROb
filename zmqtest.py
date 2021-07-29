import redis
import pandas as pd
import numpy as np
import zmq

r = redis.Redis(host='168.36.1.170', port=6379)
SList = np.array(r.keys('IDXLST:000016.SH:*'), dtype='str')
conn_r = r.pipeline(transaction=False)
for x in SList:
    conn_r.hmget(x, ['Weight', 'ClosePx'])

tt = np.asarray(conn_r.execute(), dtype='float')
for i in range(len(SList)):
    SList[i] = 'S' + SList[i].split(":")[2][:6]
df_datas = pd.DataFrame(tt, index=SList, columns=['Weight', 'ClosePx'])

conn_r.close()
r.connection_pool.disconnect()
print(df_datas)
# 以上为一次性初始化 快照

cnt = 0

# 以下为实时数据 订阅
r = redis.Redis(host='168.36.1.151', port=6379)
conn_r = r.pipeline(transaction=False)
context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect("tcp://168.36.1.181:6666")
socket.setsockopt(zmq.SUBSCRIBE, 'MDTKS510050'.encode('utf-8'))  # 消息过滤  只接受123开头的信息
# for stock in SList:
#     key = 'KZ:' + stock + ':LATEST'
#     conn_r.get(key)
#
# res['currP'] = conn_r.execute()
# print(res)
while True:
    response = socket.recv().decode('utf-8')
    cnt += 1
    print(cnt)
    print(response)
    if cnt > 2:
        break
    # for stock in SList:
    #     key = 'KZ:'+stock+':LATEST'
    #     conn_r.get(key)
    #
    # res['currP'] = conn_r.execute()
    #
    # if cnt == 1:
    #     print(res)
    # else:
    #     pass

    # cnt += 1
