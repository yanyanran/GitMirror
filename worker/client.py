import grpc
import subprocess
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
    clone_disk_path: str
    # ...其他信息

workerID = 0
disk_path = ' '
worker = selfWorker(worker_id=-1,uuid='',urls=[], clone_disk_path='.')
selfdb = shelve.open('self.db')  

# 判断网络连接问题
def std_neterr(stderr):
    err = 'failed: The TLS connection was non-properly terminated.'
    err2 = 'Could not resolve host: gitee.com'
    if err in stderr :
        return True
    elif err2 in stderr:
        return True
    else:
        return False

def add_repo(add_repos, clone_disk_path):
    i=0
    for v in add_repos:
        if v in worker.urls :
            print(v," is cloned")
            i=1
        else:  # 需要clone
            worker.urls.append(v)
            print('开始clone...')
            command = "cd %s && git clone %s" %(clone_disk_path, v)
            result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            if result.returncode != 0:
                if std_neterr(result.stderr):
                        print("repo克隆失败.... ")
                        time.sleep(2)
                        continue
            print("repo克隆成功! ")
    for v in worker.urls:
        if v not in add_repos :
            print("remove ",v)
            worker.urls.remove(v)
            # TODO 命令行rm本地仓库
    
    selfdb["worker"] = worker
    
def del_repo(del_repos):
    for v in del_repos:
        print(v)

def HeartBeat(stub, threadPool):
    while True:
        try:
            #print('send heartBeat id: ', workerID)
            res = stub.HeartBeat(func_pb2.HeartBeatRequest(workerID = workerID))
            if res.status == common.ADD_DEL_REPO:
                if res.add_repos:  # add_repos为空
                    threadPool.submit(add_repo,res.add_repos, worker.clone_disk_path)
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
    global worker
    global workerID 
    try:
        coor_ip_addr = configMap.get('where_coor_ip_addr')
        clone_disk_path = configMap.get('clone_disk_path')
        if not os.path.exists(clone_disk_path):
            os.makedirs(clone_disk_path)
        worker.clone_disk_path = clone_disk_path
        channel = grpc.insecure_channel(coor_ip_addr) # 连接rpc server
        stub = func_pb2_grpc.CoordinatorStub(channel) # 调用rpc服务
        if selfdb: 
            worker = selfdb["worker"]
            workerID = worker.worker_id
        response = stub.SayHello(func_pb2.HelloRequest(workerID=worker.worker_id,uuid=worker.uuid))
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
                return
            else: 
                print(f"gRPC Error: {e.details()}")
                return
    
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