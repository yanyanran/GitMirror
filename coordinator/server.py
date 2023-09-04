from concurrent import futures
import time
import grpc
import os
import threading
import sys
sys.path.append('../protocal/')
import func_pb2
import func_pb2_grpc
import yaml
import pathlib
import subprocess
from bitarray import bitarray
from dataclasses import dataclass

@dataclass
class Worker:
    worker_id: int
    heartBeatStep: int
    alive: bool
    # ...其他信息

BITMAP_MAX_NUM = 1000   # 管理worker的bitmap默认大小（TODO：从配置文件读/写死）
HEARTBEAT_INTERVAL = 10

workers = {}  # 存放所有worker（TODO：cache持久化）
bitmap = bitarray(BITMAP_MAX_NUM)

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

# 实现 proto 文件中定义的 Servicer
class Coordinator(func_pb2_grpc.CoordinatorServicer):
    # 实现 proto 文件中定义的 rpc 调用
    def SayHello(self, request, context):
        id = get_free_id(bitmap)
        worker = Worker(worker_id = id, heartBeatStep = 0, alive = True)
        workers[id] = worker
        print(request.w_to_s_msg, end = " ")
        print("worker[%d]已连接!" %id)
        return func_pb2.HelloResponse(s_to_w_msg = '【连接coordinator成功】your worker_ID is', workerID = id)
    
    def HeartBeat(self, request, context):
        print("收到 work: ", request.workerID ,"的心跳")
        worker = workers.get(request.workerID)
        worker.heartBeatStep = 0
        # TODO：读写map需加锁（后期维护在coordinator类里）
        return func_pb2.HeartBeatResponse()

def dealTimeout():
    while True:
        tmp_workers = dict(workers)
        for v in tmp_workers.values():
            #if v.alive == False:
            #    continue
            v.heartBeatStep += 1 # TODO：读写map需加锁（后期维护在coordinator类里）
            if v.heartBeatStep >= HEARTBEAT_INTERVAL:
                print("【Worker Timeout】worker %d 连接超时!" %v.worker_id)
                # TODO: 等待重连10min
                v.alive = False
                release_id(bitmap, v.worker_id)
                workers.pop(v.worker_id)    # TODO: worker全局不delete
        time.sleep(5)
    
# 读取配置文件               
def read_config_file():
    curPath = os.path.dirname(os.path.realpath(__file__))
    yamlPath = os.path.join(curPath, "config.yaml")
    
    with open(yamlPath, 'r', encoding='utf-8') as f:
        config = f.read()
        
    configMap = yaml.load(config,Loader=yaml.FullLoader)
    repos_name=configMap.get('upstream_repos_name')
    repos_url=configMap.get('upstream_repos_url')

    # 获取绝对路径
    reposPath = os.path.join(curPath, repos_name)
    while True:
        if os.path.exists(reposPath) == False:
            print("文件不存在！")
            command = "git clone %s" % repos_url
            result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            if result.returncode != 0:
                if std_neterr(result.stderr):
                    print("upstream仓库克隆失败.... ")
                    time.sleep(2)
                    continue
            print("upstream仓库克隆成功! ")
        else:
            print("文件存在！")
            command = "cd %s && git pull" % reposPath
            result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            # git pull错误情况解析判断
            # 1、网断：隔一段时间反复重试
            # 2、git_url失效：删除原来的本地仓库、重新读取配置文件获取最新url、重新clone
            if result.returncode != 0:
                if std_neterr(result.stdout):
                    continue
                else:
                    os.remove(reposPath)
                    continue
       
        return configMap

# 判断网络连接问题
def std_neterr(stderr):
    err='failed: The TLS connection was non-properly terminated.'
    if err in stderr :
        return True
    else :
        return False 
       
def build_git_tree(configMap):
    git_tree = {}
    curPath = os.path.dirname(os.path.realpath(__file__))
    reposPath = os.path.join(curPath, configMap.get('upstream_repos_name'))
    
    for i in range(26):
        letter = chr(i + 97)
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
    return git_tree           
    
def startup():
    configMap = read_config_file()
    git_tree = build_git_tree(configMap)
    print(git_tree.get("a").get("abc"))

def rpc_serve():
    # 启动 rpc 服务
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    func_pb2_grpc.add_CoordinatorServicer_to_server(Coordinator(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    try:
        while True:
            time.sleep(60*60*24) # one day in seconds
    except KeyboardInterrupt:
        server.stop(0)

def serve():
    startup()
    rpc_serve()

if __name__ == '__main__':
    heartbeat=threading.Thread(target=dealTimeout)
    heartbeat.start()
    serve()