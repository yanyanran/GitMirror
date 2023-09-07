import grpc
import time
import threading
import os
import yaml
import shelve
from concurrent import futures
import sys
sys.path.append('../protocal/')
import func_pb2
import func_pb2_grpc
import common
from dataclasses import dataclass
from typing import List

@dataclass
class selfWorker:
    worker_id: int
    uuid: str
    urls: List[str] # worker维护的url list
    # ...其他信息

workerID = 0
disk_path = '/'
worker = selfWorker(worker_id=-1,uuid='',urls=[])
selfdb = shelve.open('self.db')  

def add_repo(add_repos):
    for v in add_repos:
        if v in worker.urls :
            print(v," is cloned")
        else:
            print(v)
            worker.urls.append(v)
    for v in worker.urls:
        if v not in add_repos :
            print("remove ",v)
            worker.urls.remove(v)
    
    selfdb["worker"] = worker
    
def del_repo(del_repos):
    for v in del_repos:
        print(v)

def HeartBeat(stub, threadPool):
    while True:
        try:
            res = stub.HeartBeat(func_pb2.HeartBeatRequest(workerID = workerID))
            if res.status == common.ADD_DEL_REPO:
                if res.add_repos:  # add_repos为空
                    threadPool.submit(add_repo,res.add_repos)
                if res.del_repos:
                    threadPool.submit(del_repo,res.del_repos)
                
            time.sleep(5)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                break
            else:
                print(f"gRPC Error: {e.details()}")
                break

# 读取配置文件
def read_config_file():
    curPath = os.path.dirname(os.path.realpath(__file__))
    yamlPath = os.path.join(curPath, "config.yaml")
    with open(yamlPath, 'r', encoding='utf-8') as f:
        config = f.read()
    configMap = yaml.load(config,Loader=yaml.FullLoader)
    return configMap

def conn_to_coordinator(configMap):
    try:
        coor_ip_addr = configMap.get('where_coor_ip_addr')
        channel = grpc.insecure_channel(coor_ip_addr) # 连接rpc server
        stub = func_pb2_grpc.CoordinatorStub(channel) # 调用rpc服务
        if  selfdb: 
            global worker
            worker = selfdb["worker"]
        response = stub.SayHello(func_pb2.HelloRequest(workerID=worker.worker_id,uuid=worker.uuid))
        global workerID 
        if response.uuid != worker.uuid:
            workerID = response.workerID
            worker.worker_id = response.workerID
            worker.uuid = response.uuid
            # TODO del old repos前添加一个操作：
            
            # TODO DEL old repos
            worker.urls = []
        selfdb["worker"] = worker
        print('连接coordinator成功! workerID:', worker.worker_id,'uuid:',worker.uuid)
    except grpc.RpcError as e:  # 处理gRPC异常
            if e.code() == grpc.StatusCode.UNAVAILABLE:  # 服务器未启动
                print("Error: gRPC Server is not available. Make sure the server is running.")
            else: 
                print(f"gRPC Error: {e.details()}")
    
    # 添加线程池
    threadPool = futures.ThreadPoolExecutor(max_workers=10)

    # HeartBeat线程
    heartbeat = threading.Thread(target = HeartBeat,args = (stub,threadPool))
    heartbeat.start()
    while True:
        i=0

    

if __name__ == '__main__':
    configMap = read_config_file()
    conn_to_coordinator(configMap)