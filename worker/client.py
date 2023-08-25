import grpc
import time
import threading
import sys
sys.path.append('../protocal/')
import func_pb2
import func_pb2_grpc

workerID = 0

def HeartBeat(stub):
    while True:
        stub.HeartBeat(func_pb2.HeartBeatRequest(workerID = workerID))
        time.sleep(5)

def conn_to_coordinator():
    channel = grpc.insecure_channel('localhost:50051') # 连接rpc server
    stub = func_pb2_grpc.CoordinatorStub(channel) # 调用rpc服务
    
    response = stub.SayHello(func_pb2.HelloRequest(w_to_s_msg='【Worker Connected】'))
    global workerID 
    workerID = response.workerID
    print(response.s_to_w_msg, end = " ")
    print(workerID)
    
    heartbeat = threading.Thread(target = HeartBeat,args = (stub,))
    heartbeat.start()
    

if __name__ == '__main__':
    conn_to_coordinator()