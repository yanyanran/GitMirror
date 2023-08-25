from concurrent import futures
import time
import grpc
import threading
import sys
sys.path.append('../protocal/')
import func_pb2
import func_pb2_grpc
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

workers = {}  # 存放所有worker
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
                
            

def serve():
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

if __name__ == '__main__':
    heartbeat=threading.Thread(target=dealTimeout)
    heartbeat.start()
    serve()