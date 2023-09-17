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
from queue import PriorityQueue
import math
import time
import re

@dataclass
class SelfWorker:
    worker_id: int
    uuid: str
    urls: List[str] # worker维护的url list
    clone_disk_path: str
    # ...其他信息

@dataclass
class Task:
    def __init__(self):
        self.pri = 0
        self.task_type = ''
        self.repo_url = ''
        self.clone_disk_path = ''
        self.clone_fail_cnt = 0
        self.fetch_fail_cnt = 0
        self.step = 0
        self.STEP_SECONDS = 150000  # 每个步骤的时间间
        self.last_commit_time = 0
        
    def clone_repo(self, task_queue):
        print('开始尝试clone...')
        command = "cd %s && git clone %s" %(self.clone_disk_path, self.repo_url)
        result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
              
        if result.returncode != 0:
            if common.std_neterr(result.stderr) == 1:  # net error
                print("[clone] repo克隆失败, 稍后重试.... ")
                self.clone_fail_cnt += 1
            elif common.std_cloneerr(result.stderr) == 2: # is cloned
                print("[clone] repo is clone, 开始尝试更新仓库... ")
                self.task_type = 'git fetch'
            elif common.std_cloneerr(result.stderr) == 1: # url无效
                print('[clone] clone fail: 仓库url无效')
                return
            
        self.task_type = 'git fetch'   
        self.get_task_priority()
        task_queue.put([self.pri, self])   
        print("repo克隆成功! ")
        
    def fetch_repo(self, task_queue):
        print('start fetch ',self.repo_url)
        command = "cd %s && git fetch %s" %(self.clone_disk_path, self.repo_url)
        result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

        if result.returncode != 0:
            if common.std_fetcherr(result.stderr) == 1:
                print('[fetch] 当前仓库不存在！')
                return
            # 仓库存在但fetch失败 (重试
            self.fetch_fail_cnt += 1
        else:
            print('仓库fetch成功!')
        
        self.get_task_priority()
        task_queue.put([self.pri, self])
    
    # 获取优先级
    def get_task_priority(self):
        if self.pri is None:
            self.pri = 0
        step = (self.clone_fail_cnt + 1) * math.pow(self.STEP_SECONDS, 1/3)
    
        if self.task_type == 'git clone': # clone task
            self.pri += step  # 默认step
            return
        return self.cal_priority()
    
    # [fetch]根据仓库最后一次提交的时间与当前时间的间隔来计算优先级
    def cal_priority(self):
        old_pri = self.pri
        step = (self.fetch_fail_cnt + 1) * math.pow(self.STEP_SECONDS, 1/3)
        
        if self.last_commit_time == -1:  # time前置获取失败
            WorkerManager.get_last_commit_time(self.repo_url)
        elif self.last_commit_time == 0:  # last_commit_time为零-> 没进行过提交，返回默认step
            self.pri = old_pri + step
            return

        # 最近更新的仓库优先级较高，从而更快地获取到更新
        t = time.time()
        interval = t - self.last_commit_time  # 计算当前时间与最后一次提交时间之间的时间间隔
        if interval <= 0:
            self.pri = old_pri + step
            return

        self.pri = old_pri + math.pow(interval, 1/3)
        return

class WorkerManager:
    def __init__(self):
        self.worker = SelfWorker(worker_id=-1, uuid='', urls=[], clone_disk_path='.')
        self.selfdb = shelve.open('self.db')
        if "worker" in self.selfdb:
            self.worker = self.selfdb["worker"]
        self.task_queue = PriorityQueue()  # 优先级队列
        for i in  self.worker.urls:
            task = Task()
            task.repo_url = i  
            task.task_type = 'git fetch'
            task.clone_disk_path = self.worker.clone_disk_path
            task.pri = 1
            self.task_queue.put([task.pri, task]) # input priqueue 
                
    def get_last_commit_time(self, repo):
        try:
            name = self.get_name_from_repo(repo)
            command = f"cd {self.worker.clone_disk_path} && git -C {name} log --pretty=format:%ct -1"
            output = subprocess.check_output(command, shell=True, stderr=subprocess.DEVNULL, universal_newlines=True)
            self.last_commit_time = int(output.strip())  # 将输出的时间戳字符串转换为整数
            return self.last_commit_time
        except subprocess.CalledProcessError:   # 处理命令执行失败的情况
            self.last_commit_time = -1
            return None
        
    # 使用正则表达式从url中提取仓库名
    def get_name_from_repo(self, git_url):
        match = re.search(r'/([^/]+?)(?:\.git)?$', git_url)
        if match:
            return match.group(1)
        else:
            return None
    
    def add_repo(self, add_repos):
        for v in add_repos:
            if v in self.worker.urls:
                print('[add repo] 存在repo', v)
                continue
            else:
                self.worker.urls.append(v)
            task = Task()
            task.repo_url = v
            task.task_type = 'git clone'
            task.clone_disk_path = self.worker.clone_disk_path
            task.pri = 1
            self.task_queue.put([task.pri, task]) # input priqueue
        
        rm_list = []
        for v in self.worker.urls:
            if v not in add_repos:  # 删除不维护的本地仓库
                print("开始remove repo", v)
                rm_list.append(v)
        self.del_repo(rm_list)

        self.selfdb["worker"] = self.worker # 写磁盘

    def del_repo(self, del_repos):
        for v in del_repos:
            print('remove %s' %v)
            self.worker.urls.remove(v)
            repo_name = self.get_name_from_repo(v)
            command = 'cd %s && rm -rf %s' %(self.worker.clone_disk_path, repo_name)
            result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            if result.returncode != 0:
                print('rm repo fail!') 
            print("rm repo success!")

        self.selfdb["worker"] = self.worker # 写磁盘
    
    # 从priqueue取一个task解析并执行
    def process_tasks(self, thread_pool):
        while True:
            _, task = self.task_queue.get()
            if task.repo_url in self.worker.urls:
                if task.task_type == "git clone":
                    thread_pool.submit(task.clone_repo, self.task_queue)
                else:
                    thread_pool.submit(task.fetch_repo,self.task_queue)  
    
    def heart_beat(self, stub):
        while True:
            try:
                res = stub.HeartBeat(func_pb2.HeartBeatRequest(workerID=self.worker.worker_id))
                if res.status == common.ADD_DEL_REPO:
                    if res.add_repos:  # add_repos为空
                        self.add_repo(res.add_repos)
                    if res.del_repos:
                        self.del_repo(res.del_repos)
                time.sleep(5)
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    print('检测到coordinator挂机!')
                    sys.exit(0)
                else:
                    print(f"gRPC Error: {e.details()}")
                    sys.exit(0)

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
                self.worker.urls = []
            self.selfdb["worker"] = self.worker
            print('连接coordinator成功! workerID:', self.worker.worker_id, 'uuid:', self.worker.uuid)
            
            # 添加线程池(task thread用)
            thread_pool = futures.ThreadPoolExecutor(max_workers=10)
            taskThread = threading.Thread(target=self.process_tasks, args=(thread_pool, ))
            taskThread.start()

            self.heart_beat(stub)
            
        except grpc.RpcError as e:  # 处理gRPC异常
            if e.code() == grpc.StatusCode.UNAVAILABLE: # 服务器未启动
                print("Error: gRPC Server is not available. Make sure the server is running.")
            else:
                print(f"gRPC Error: {e.details()}")

if __name__ == '__main__':
    manager = WorkerManager()
    config_map = manager.read_config_file()
    manager.connect_to_coordinator(config_map)