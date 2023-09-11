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
class SelfWorker:
    worker_id: int
    uuid: str
    urls: List[str] # worker维护的url list
    clone_disk_path: str
    # ...其他信息

class WorkerManager:
    def __init__(self):
        self.worker = SelfWorker(worker_id=-1, uuid='', urls=[], clone_disk_path='.')
        self.selfdb = shelve.open('self.db')
        if "worker" in self.selfdb:
            self.worker = self.selfdb["worker"]

    def add_repo(self, add_repos):
        for v in add_repos:
            if v in self.worker.urls:
                print(v, " is cloned")  # run: hash_distribute_urls
            else:  # 需要clone
                self.worker.urls.append(v)
                print('开始clone...')
                command = "cd %s && git clone %s" % (self.worker.clone_disk_path, v)
                result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
                
                if result.returncode != 0:
                    if common.std_neterr(result.stderr):
                        print("repo克隆失败.... ")
                        time.sleep(2)
                        continue
                    
                print("repo克隆成功! ")

        for v in self.worker.urls:
            if v not in add_repos:
                print("remove ", v)
                self.worker.urls.remove(v)
                # TODO 命令行rm本地仓库

        self.selfdb["worker"] = self.worker # 写磁盘

    def del_repo(self, del_repos):
        for v in del_repos:
            print(v)

    def heart_beat(self, stub, thread_pool):
        while True:
            try:
                res = stub.HeartBeat(func_pb2.HeartBeatRequest(workerID=self.worker.worker_id))
                if res.status == common.ADD_DEL_REPO:
                    if res.add_repos:  # add_repos为空
                        thread_pool.submit(self.add_repo, res.add_repos)
                    if res.del_repos:
                        thread_pool.submit(self.del_repo, res.del_repos)
                time.sleep(5)
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    break
                else:
                    print(f"gRPC Error: {e.details()}")
                    break

    @staticmethod
    def read_config_file():
        cur_path = os.path.dirname(os.path.realpath(__file__))
        yaml_path = os.path.join(cur_path, "config.yaml")
        with open(yaml_path, 'r', encoding='utf-8') as f:
            config = f.read()
        config_map = yaml.load(config, Loader=yaml.FullLoader)
        return config_map

    def connect_to_coordinator(self, config_map):
        try:
            coor_ip_addr = config_map.get('where_coor_ip_addr')
            clone_disk_path = config_map.get('clone_disk_path')
            if not os.path.exists(clone_disk_path):
                os.makedirs(clone_disk_path)
            self.worker.clone_disk_path = clone_disk_path
            channel = grpc.insecure_channel(coor_ip_addr) # 连接rpc server
            stub = func_pb2_grpc.CoordinatorStub(channel) # 调用rpc服务
            response = stub.SayHello(func_pb2.HelloRequest(workerID=self.worker.worker_id, uuid=self.worker.uuid))
            if response.uuid != self.worker.uuid:
                self.worker.worker_id = response.workerID
                self.worker.uuid = response.uuid
                # TODO del old repos前添加一个操作：
            
                # TODO DEL old repos
                self.worker.urls = []
            self.selfdb["worker"] = self.worker
            print('连接coordinator成功! workerID:', self.worker.worker_id, 'uuid:', self.worker.uuid)
            
            # 添加线程池
            thread_pool = futures.ThreadPoolExecutor(max_workers=10)
            
            # HeartBeat线程
            heartbeat = threading.Thread(target=self.heart_beat, args=(stub, thread_pool))
            heartbeat.start()
            while True:
                pass
            
        except grpc.RpcError as e:  # 处理gRPC异常
            if e.code() == grpc.StatusCode.UNAVAILABLE: # 服务器未启动
                print("Error: gRPC Server is not available. Make sure the server is running.")
            else:
                print(f"gRPC Error: {e.details()}")

if __name__ == '__main__':
    manager = WorkerManager()
    config_map = manager.read_config_file()
    manager.connect_to_coordinator(config_map)