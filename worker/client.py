import grpc
import time
import sys
sys.path.append('../protocal/')
import func_pb2
import func_pb2_grpc

def run():
    channel = grpc.insecure_channel('localhost:50051') # 连接rpc server
    stub = func_pb2_grpc.CoordinatorStub(channel) # 调用rpc服务
    
    response = stub.SayHello(func_pb2.HelloRequest(name='【Connected】I am a wrorker.'))
    print("worker client" + response.message)
    
    time.sleep(10)

if __name__ == '__main__':
    run()