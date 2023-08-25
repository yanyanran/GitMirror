from concurrent import futures
import time
import grpc
import sys
sys.path.append('../protocal/')
import func_pb2
import func_pb2_grpc
from bitarray import bitarray

BITMAP_MAX_NUM = 1000   # 管理worker的bitmap默认大小（从配置文件读/写死）
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
        worker_id = get_free_id(bitmap)
        print("worker[%d]已连接!" %worker_id)
        return func_pb2.HelloReply(msg = 'your worker_ID is  {msg}'.format(msg = request.name))

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
    serve()