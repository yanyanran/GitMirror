from concurrent import futures
import time
import grpc
import os
import threading
import sys
import uuid
import subprocess
import shelve
import yaml
import pathlib
from dataclasses import dataclass
from typing import List
from bitarray import bitarray
import hash_ring
import sys
sys.path.append('../protocal/')
import func_pb2
import func_pb2_grpc
import common
from flask import Flask, request, jsonify
@dataclass
class Worker:
    worker_id: int
    heartBeatStep: int
    alive: bool
    uuid: str
    urls: List[str]

@dataclass
class UrlsArray:
    add_urls: List[str]
    del_urls: List[str]
    add_urls_lock: threading.Lock
    del_urls_lock: threading.Lock
    
    dump_add_again_url: List[str]
    dump_add_again_url_lock: threading.Lock

class CoordinatorServer:
    def __init__(self, config_map):
        self.BITMAP_MAX_NUM = 1000
        self.HEARTBEAT_INTERVAL = 4
        self.HEARTBEAT_DIEOUT = 8
        self.FETCH_INTERVAL = 10
        self.bitmap = bitarray(self.BITMAP_MAX_NUM)
        self.workers = {}
        self.urls_cache = {}  # send urls cache
        self.git_tree = {}
        self.hashring = hash_ring.HashRing()
        self.workers_db = shelve.open('workers.db')
        self.db_lock = threading.Lock()
        self.git_tree_lock = threading.Lock()
        self.workers_map_lock = threading.Lock()
        self.config_map = config_map
    
    def get_free_id(self):
        for i, allocated in enumerate(self.bitmap):
            if not allocated:
                self.bitmap[i] = True
                return i
        raise ValueError("No free ID available.")

    def release_id(self, id):
        if id < len(self.bitmap):
            self.bitmap[id] = False
        else:
            raise ValueError("Invalid ID.")

    def build_git_tree(self):
        curPath = os.path.dirname(os.path.realpath(__file__))
        
        for url in self.config_map.get('upstream_repos_urls'):
            repo_name = common.get_name_from_repo(url)
            reposPath = os.path.join(curPath, repo_name)
            with self.git_tree_lock:
                for _, dirs, _ in os.walk(reposPath):
                    for directory in dirs:
                        if directory.startswith('.'):
                            continue
                        repo_tree = {}
                        self.git_tree[directory] = repo_tree
                        path = os.path.join(reposPath, directory)
                        for _, dirs, _ in os.walk(path):
                            for directory in dirs:
                                path1 = os.path.join(path, directory)
                                path1 = os.path.join(path1, directory)
                                with open(path1, 'r', encoding='utf-8') as f:
                                    repo = f.read()
                                    repoMap = yaml.load(repo,Loader=yaml.FullLoader)
                                    repo_tree[directory] = repoMap.get("url")
                            break
                    break

    def get_new_tree(self):
        curPath = os.path.dirname(os.path.realpath(__file__))
        git_tree = {}
        
        for url in self.config_map.get('upstream_repos_urls'):
            repo_name = common.get_name_from_repo(url)
            reposPath = os.path.join(curPath, repo_name)
            for _, dirs, _ in os.walk(reposPath):
                for directory in dirs:
                    if directory.startswith('.'):
                        continue
                    repo_tree = {}
                    self.git_tree[directory] = repo_tree
                    path = os.path.join(reposPath, directory)
                    for _, dirs, _ in os.walk(path):
                        for directory in dirs:
                            path1 = os.path.join(path, directory)
                            path1 = os.path.join(path1, directory)
                            with open(path1, 'r', encoding='utf-8') as f:
                                repo = f.read()
                                repoMap = yaml.load(repo,Loader=yaml.FullLoader)
                                repo_tree[directory] = repoMap.get("url")
                        break
                break
        
        return git_tree
    
    # 检查coor本地是否存有worker缓存
    def checkHaveWorkerCache(self):
        with self.db_lock:
            if not self.workers_db:  # 等待worker连接10min
                print('no cache!')    
            else: # 加载到内存中，然后等待10min进行状态重连
                print('have cache!')
                for k, v in self.workers_db.items():
                    v.alive = False
                    v.heartBeatStep = 0
                    with self.workers_map_lock:
                        self.workers[v.worker_id] = v
                    self.bitmap[v.worker_id] = True
                    array = UrlsArray(add_urls=[], del_urls=[],add_urls_lock= threading.Lock(),del_urls_lock= threading.Lock(),dump_add_again_url=[],dump_add_again_url_lock=threading.Lock())   
                    self.urls_cache[v.worker_id] = array 
            # time.sleep(600)
        
    def init_hashring(self):
         for value in self.workers.values():
            node = value.worker_id
            self.hashring.add_node(node)  # 添加workerID到环上作为ring node
            
    def mainLoop_serve(self):
        self.build_git_tree()
        print('git_tree build ok!')
        self.init_hashring()
        self.hash_distribute_urls()
        print('distribute repo to worker is ok!')
        
        while True:  # 定期fetch上游总仓库
            if self.config_map.get('fetch_interval') != None:
                self.FETCH_INTERVAL = self.config_map.get('fetch_interval')
                
            for url in self.config_map.get('upstream_repos_urls'):    
                curPath = os.path.dirname(os.path.realpath(__file__))
                repo_name = common.get_name_from_repo(url)
                reposPath = os.path.join(curPath, repo_name)
                command = f'cd {reposPath} && git pull'
                result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
                
                if result.returncode == 0:
                    if 'Already up to date.'in result.stdout or  '已经是最新的。' in result.stdout:
                        print('result.stdout: ', result.stdout)
                        pass
                    else:
                        print('已更新上游仓库变动！')
                        new_tree = self.get_new_tree()
                        with self.git_tree_lock: 
                            self.git_tree = new_tree
                        self.hash_distribute_urls()
                else:
                    print(f"拉取远程仓库更新失败!")
                    
                time.sleep(self.FETCH_INTERVAL)

    def serve(self, ipaddr):
        heartbeat = threading.Thread(target=self.deal_timeout)
        heartbeat.start()
        self.coor_serve(ipaddr)

    def coor_serve(self, ipaddr):
        print('start rpc...')
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        func_pb2_grpc.add_CoordinatorServicer_to_server(Coordinator(self), server)
        server.add_insecure_port(ipaddr)
        server.start()
        try:
            while True:
                time.sleep(60*60*24)
        except KeyboardInterrupt:
            server.stop(0)

# 根据hashring, 遍历git_tree开始分配url到每个worker的cache上
# 后续：(add)upstream repo有更新，增/减repo
#           有worker挂掉，将repo分配给其他worker
#           (del)有worker恢复
    def hash_distribute_urls(self):
        with self.git_tree_lock:
            for _, sub_tree in self.git_tree.items():
                for k, urls in sub_tree.items():
                    for url in urls:
                        nodeID = self.hashring.get_urls_node(k)
                        if nodeID==-1:
                            break
                        with self.urls_cache[nodeID].add_urls_lock:
                            self.urls_cache[nodeID].add_urls.append(url) 
    
    # 每次重连，coor向worker发送所属worker维护的urls                      
    def send_hash_urls(self, workerID):
        with self.git_tree_lock:
            for _, sub_tree in self.git_tree.items():
                for k, urls in sub_tree.items():
                    for url in urls:
                        nodeID = self.hashring.get_urls_node(k)
                        if nodeID == -1 or nodeID != workerID:
                            break
                        with self.urls_cache[nodeID].add_urls_lock:
                            self.urls_cache[nodeID].add_urls.append(url) 

    def deal_timeout(self):
        while True:
            with self.workers_map_lock:
                tmp_workers = dict(self.workers)
            for v in tmp_workers.values():
                v.heartBeatStep += 1
                if v.heartBeatStep == self.HEARTBEAT_INTERVAL:
                    print(f"【Worker Timeout】worker {v.worker_id} 连接超时!")
                    v.alive = False
                if v.heartBeatStep >= self.HEARTBEAT_DIEOUT:
                    print(f"【Worker Dieout】worker {v.worker_id} 挂了!拜拜！")
                    self.hashring.remove_node(v.worker_id)
                    self.hash_distribute_urls()
                    self.release_id(v.worker_id)
                    with self.workers_map_lock:
                        self.workers.pop(v.worker_id)
                    with self.db_lock:
                        del self.workers_db[str(v.worker_id)]
                    del self.urls_cache[v.worker_id]
            time.sleep(5)

class Coordinator(func_pb2_grpc.CoordinatorServicer):
    def __init__(self, server):
        self.server = server
        
    def SayHello(self, request, context):
        old_workerID = request.workerID       
        with self.server.workers_map_lock:
            if old_workerID in self.server.workers:
                old_uuid = request.uuid
                if old_uuid == self.server.workers[old_workerID].uuid:  # worker重连
                    self.server.workers[old_workerID].heartBeatStep = 0
                    if old_workerID in self.server.urls_cache:
                        l = self.server.urls_cache[old_workerID]
                        if (len(l.add_urls)!= 0 or len(l.del_urls)!= 0):
                            pass
                        else:  # map is empty
                            self.server.send_hash_urls(old_workerID)
                    return func_pb2.HelloResponse(workerID = old_workerID, uuid = old_uuid)
            id = self.server.get_free_id()
            
            worker = Worker(worker_id = id, heartBeatStep = 0, alive = True, uuid = str(uuid.uuid1()), urls=[])
            self.server.workers[id] = worker
        array = UrlsArray(add_urls=[], del_urls=[],add_urls_lock= threading.Lock(),del_urls_lock= threading.Lock(),dump_add_again_url=[],dump_add_again_url_lock=threading.Lock())
        self.server.urls_cache[id] = array 
            
        print("worker[%d]已连接!" %id)
        self.server.hashring.add_node(id)
        self.server.hash_distribute_urls()
        with self.server.db_lock:
            self.server.workers_db[str(id)] = worker
        return func_pb2.HelloResponse(workerID = id, uuid = worker.uuid)
    
    def HeartBeat(self, request, context):
        print("收到 worker: ", request.workerID ,"的心跳")
        with self.server.workers_map_lock:
            worker = self.server.workers.get(request.workerID)
        worker.heartBeatStep = 0
        worker.alive = True
        with self.server.urls_cache[request.workerID].add_urls_lock:
            add_arr = list(self.server.urls_cache[request.workerID].add_urls)
            self.server.urls_cache[request.workerID].add_urls.clear()
        with self.server.urls_cache[request.workerID].del_urls_lock:
            del_arr = list(self.server.urls_cache[request.workerID].del_urls)
            self.server.urls_cache[request.workerID].del_urls.clear()
        with self.server.urls_cache[request.workerID].dump_add_again_url_lock:
            dump_arr = list(self.server.urls_cache[request.workerID].dump_add_again_url)
            self.server.urls_cache[request.workerID].dump_add_again_url.clear()
        
        if not add_arr and not del_arr :
            return func_pb2.HeartBeatResponse(status=0,add_repos=add_arr,del_repos=del_arr,dump_repos=dump_arr)

        return func_pb2.HeartBeatResponse(status=common.ADD_DEL_REPO,add_repos=add_arr,del_repos=del_arr,dump_repos=dump_arr)
    
    def DiskFull(self, request, context):
        print(request.dump_repo + "仓库因磁盘原因被退回, 准备重新分配给其他worker维护...")
        url = request.dump_repo + "+yrd"   # 重新计算md5 重新匹配worker
        nodeID = self.server.hashring.get_urls_node(url)
        with self.urls_cache[nodeID].add_urls_lock:
            self.urls_cache[nodeID].dump_add_again_url.append(request.dump_repo) 

def download_upstream_repo(configMap):
    curPath = os.path.dirname(os.path.realpath(__file__))
    repo_list = configMap.get('upstream_repos_urls')

    for url in repo_list:
        repos_name = common.get_name_from_repo(url)

        # 获取绝对路径
        reposPath = os.path.join(curPath, repos_name)
        i = 0
        while True:
            i = i + 1
            if i > 50:
                print('当前有仓库尝试克隆50次失败! 请检查url是否正确!')
            if os.path.exists(reposPath) == False:
                print("upsteam仓库不存在!")
                command = "git clone %s" % url
                result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
                if result.returncode != 0:
                    if common.std_neterr(result.stderr) == 1:
                        print("upstream仓库克隆失败.... ")
                        time.sleep(2)
                        continue
                print("upstream仓库克隆成功! ")
            else:
                print("upsteam仓库存在!")
                command = "cd %s && git pull" % reposPath
                result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
                # git pull错误情况解析判断
                # 1、网断：隔一段时间反复重试
                # 2、git_url失效：删除原来的本地仓库、重新读取配置文件获取最新url、重新clone
                if result.returncode != 0:
                    if common.std_neterr(result.stdout) == 1:
                        continue

# 读取配置文件               
def read_config_file():
    print('start read config file...')
    curPath = os.path.dirname(os.path.realpath(__file__))
    yamlPath = os.path.join(curPath, "config.yaml")
    
    with open(yamlPath, 'r', encoding='utf-8') as f:
        config = f.read()
    configMap = yaml.load(config,Loader=yaml.FullLoader)
      
    return configMap

app = Flask(__name__)

# HTTP路由，用于向外部暴露一个接口
@app.route("/api/status", methods=["GET"])
def get_status():
    # 获取状态信息 e.g. app.run(host='0.0.0.0', port=8089)-> http://localhost:8089/api/status
    urlMap = {}
    for k,_ in server.workers.items():
        urlMap[k] = []
    with server.git_tree_lock:
        for _, sub_tree in server.git_tree.items():
            for _, urls in sub_tree.items():
                for url in urls:
                    id=server.hashring.get_urls_node(url)
                    if id==-1 :
                        return
                    urlMap[id].append(url)

    status_info = {
        "server_status": "running",
        "worker_count": len(server.workers),
        "worker:url": urlMap,
    }
    return jsonify(status_info)

if __name__ == '__main__':
    config_map = read_config_file()
    download_upstream_repo(config_map)
    server = CoordinatorServer(config_map)
    server.checkHaveWorkerCache()
    
    addr = config_map.get('coor_ip_addr') + config_map.get('coor_port')
    serve_startup = threading.Thread(target=server.serve,args=(addr,))
    serve_startup.start()
    
    server.mainLoop_serve()