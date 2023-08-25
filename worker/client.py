import grpc
import time
import sys
sys.path.append('../protocal/')
import func_pb2
import func_pb2_grpc

def conn_to_coordinator():
    channel = grpc.insecure_channel('localhost:50051') # 连接rpc server
    stub = func_pb2_grpc.CoordinatorStub(channel) # 调用rpc服务
    
    response = stub.SayHello(func_pb2.HelloRequest(name='【Worker Connected】连接coordiator成功.'))
    print(response.msg)

if __name__ == '__main__':
    conn_to_coordinator()