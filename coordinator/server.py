from concurrent import futures
import time
import grpc
import os
import threading
import sys
sys.path.append('../protocal/')  # TODO：路径启动过于绝对
import func_pb2
import func_pb2_grpc
import common
import yaml
import pathlib
import uuid
import subprocess
import hash_ring
import shelve

from bitarray import bitarray
from dataclasses import dataclass
from typing import List

# 为每个worker维护一个添加url的缓冲区

@dataclass
class Worker:
    worker_id: int
    heartBeatStep: int
    alive: bool
    uuid: str
    urls: List[str] # worker维护的url list
    # ...其他信息
    # TODO：role: worker/aggregator、pub_ip_addr（aggregator）

@dataclass
class urlsArray:
    # cache
    add_urls: List[str]
    del_urls: List[str]
    
    add_urls_lock = threading.Lock()
    del_urls_lock = threading.Lock()

BITMAP_MAX_NUM = 1000   # 管理worker的bitmap默认大小（TODO：从配置文件读/写死）
HEARTBEAT_INTERVAL = 4
HEARTBEAT_DIEOUT = 8

workersdb = shelve.open('workers.db')  
db_lock = threading.Lock()

git_tree = {}
git_tree_lock = threading.Lock()

workers = {}  # 存放所有worker
workers_map_lock = threading.Lock()

urlsCache = {} # 存放worker被分配的urls（add

bitmap = bitarray(BITMAP_MAX_NUM)

hashring = hash_ring.HashRing()

# bitmap申请空闲id 
def get_free_id(bitmap):
    for i, allocated in enumerate(bitmap):
        if not allocated:
            bitmap[i] = True
            return i
    raise ValueError("No free ID available.")

# bitmap释放占用id
def release_id(bitmap, id):
    if id < len(bitmap):
        bitmap[id] = False
    else:
        raise ValueError("Invalid ID.")

# 处理定时更新上游urlrepos、...
class mainLoop:
    def build_git_tree(self, configMap):
        curPath = os.path.dirname(os.path.realpath(__file__))
        reposPath = os.path.join(curPath, configMap.get('upstream_repos_name'))
        
        with git_tree_lock:
            for i in range(26):
                letter = chr(i + 97)  # a-z
                child_reposPath = os.path.join(reposPath, letter)
                # print(child_reposPath)
                dir_path = pathlib.Path(child_reposPath) # 指定要遍历的文件目录路径
                repo_tree = {}
                git_tree[letter] = repo_tree
                for item in dir_path.rglob('*'):
                    if item.is_dir():
                        repoPath = os.path.join(child_reposPath, item.name)  # b/bis
                        repoPath = os.path.join(repoPath, item.name)  # b/bis/bis
                        with open(repoPath, 'r', encoding='utf-8') as f:
                            repo = f.read()
                            repoMap = yaml.load(repo,Loader=yaml.FullLoader)
                            repo_tree[item.name] = repoMap.get("url") 
    
    # 检查coor本地是否存有worker缓存
    def checkHaveWorkerCache(self):
        with db_lock:
            if not workersdb:  # 等待worker连接10min
                print('no cache!')    
            else: # 加载到内存中，然后等待10min进行状态重连
                print('have cache!')
                for k, v in workersdb.items():
                    v.alive = False
                    v.heartBeatStep = 0
                    with workers_map_lock:
                        workers[v.worker_id] = v
                    bitmap[v.worker_id] = True
                    array = urlsArray(add_urls=[], del_urls=[])
                    urlsCache[v.worker_id] = array 
        
    def init_hashring(self):
         for value in workers.values():
            node = value.worker_id
            hashring.add_node(node)  # 添加workerID到环上作为ring node
            
    def mainLoop_serve(self):
        self.build_git_tree(configMap)
        #print(git_tree.get("a").get("abc"))
        print('git_tree build ok!')
        print("start mainLoop_serve...")
        self.init_hashring()
        hash_distribute_urls()
        while True:
            i = 0

# 实现 proto 文件中定义的 Servicer
class Coordinator(func_pb2_grpc.CoordinatorServicer):
    # 实现 proto 文件中定义的 rpc 调用
    def SayHello(self, request, context):
        old_workerID = request.workerID
        with workers_map_lock:
            if old_workerID in workers:
                old_uuid = request.uuid
                if old_uuid == workers[old_workerID].uuid:
                    workers[old_workerID].heartBeatStep = 0
                    return func_pb2.HelloResponse(workerID = old_workerID, uuid = old_uuid)
            id = get_free_id(bitmap)
            
            worker = Worker(worker_id = id, heartBeatStep = 0, alive = True, uuid = str(uuid.uuid1()), urls=[])
            workers[id] = worker
        array = urlsArray(add_urls=[], del_urls=[])
        urlsCache[id] = array 
            
        print("worker[%d]已连接!" %id)
        hashring.add_node(id)
        hash_distribute_urls()
        with db_lock:
            workersdb[str(id)] = worker
        return func_pb2.HelloResponse(workerID = id, uuid = worker.uuid)
    
    def HeartBeat(self, request, context):
        print("收到 worker: ", request.workerID ,"的心跳")
        with workers_map_lock:
            worker = workers.get(request.workerID)
        worker.heartBeatStep = 0
        worker.alive = True
        with urlsCache[request.workerID].add_urls_lock:
            with urlsCache[request.workerID].del_urls_lock:
                add_arr =  list(urlsCache[request.workerID].add_urls)
                del_arr =  list(urlsCache[request.workerID].del_urls)
                urlsCache[request.workerID].add_urls.clear()
                urlsCache[request.workerID].del_urls.clear()
        if not add_arr and not del_arr :
            return func_pb2.HeartBeatResponse(status=0,add_repos=add_arr,del_repos=del_arr)

        return func_pb2.HeartBeatResponse(status=common.ADD_DEL_REPO,add_repos=add_arr,del_repos=del_arr)

def dealTimeout():
    print('start deal heartbeat...')
    while True:
        with workers_map_lock:
            tmp_workers = dict(workers)
        for v in tmp_workers.values():
            v.heartBeatStep += 1 # TODO：读写map需加锁（后期维护在coordinator类里）
            if v.heartBeatStep == HEARTBEAT_INTERVAL:
                print("【Worker Timeout】worker %d 连接超时!" %v.worker_id)
                v.alive = False
                
            if v.heartBeatStep >= HEARTBEAT_DIEOUT:
                print("【Worker Dieout】worker %d 挂了!拜拜！" %v.worker_id)
                hashring.remove_node(v.worker_id)
                hash_distribute_urls()
                release_id(bitmap, v.worker_id)
                with workers_map_lock:
                   workers.pop(v.worker_id)    # TODO: worker全局不delete
                with db_lock:
                    del workersdb[str(v.worker_id)]
                del urlsCache[v.worker_id]
        time.sleep(5)
    
# 读取配置文件               
def read_config_file():
    print('start read config file...')
    curPath = os.path.dirname(os.path.realpath(__file__))
    yamlPath = os.path.join(curPath, "config.yaml")
    
    with open(yamlPath, 'r', encoding='utf-8') as f:
        config = f.read()
        
    configMap = yaml.load(config,Loader=yaml.FullLoader)
    repos_name = configMap.get('upstream_repos_name')
    repos_url = configMap.get('upstream_repos_url')

    # 获取绝对路径
    reposPath = os.path.join(curPath, repos_name)
    while True:
        if os.path.exists(reposPath) == False:
            print("upsteam仓库不存在!")
            command = "git clone %s" % repos_url
            result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            if result.returncode != 0:
                if common.std_neterr(result.stderr):
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
                if common.std_neterr(result.stdout):
                    continue
       
        return configMap

# 根据hashring, 遍历git_tree开始分配url到每个worker的cache上
# TODO 后续：(add)upstream repo有更新，增/减repo
#           有worker挂掉，将repo分配给其他worker
#           (del)有worker恢复
def hash_distribute_urls():
    with git_tree_lock:
        for i in range(26):
            letter = chr(i + 97)
            sub_tree = git_tree.get(letter)
            for k, urls in sub_tree.items():
                for url in urls:
                    nodeID = hashring.get_urls_node(k)
                    if nodeID==-1:
                        break
                    with urlsCache[nodeID].add_urls_lock:
                        urlsCache[nodeID].add_urls.append(url)   

def coor_serve(ipaddr):
    print('start rpc...')
    # 启动 rpc 服务
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    func_pb2_grpc.add_CoordinatorServicer_to_server(Coordinator(), server)
    
    server.add_insecure_port(ipaddr)
    server.start()
    try:
        while True:
            time.sleep(60*60*24) # one day in seconds
    except KeyboardInterrupt:
        server.stop(0)

def serve(ipaddr):
    heartbeat = threading.Thread(target=dealTimeout)
    heartbeat.start()
    
    coor_serve(ipaddr)

if __name__ == '__main__':
    configMap = read_config_file()
    
    mainLoop_serve = mainLoop()
    mainLoop_serve.checkHaveWorkerCache()
    
    serve_startup = threading.Thread(target=serve, args=(configMap.get('coor_ip_addr'),))
    serve_startup.start()
    
    mainLoop_serve.mainLoop_serve()