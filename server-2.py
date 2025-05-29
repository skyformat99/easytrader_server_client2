from flask import Flask, request, jsonify
import flask
import easytrader
import time
import hashlib
import uuid
import threading
import queue
from types import MethodType
from gevent import pywsgi

'''
config
'''
user = easytrader.use('htzq_client')  # 需要根据自身情况修改
user.connect('C:\htzq\海通证券金融终端独立下单2.0/xiadan.exe')  # 需要根据自身情况修改
password = 'your password'  # 填入你的通信密码，用于加密通信
port = 5000  # 服务端口

app = Flask(__name__)

# 消息队列和锁
task_queue = queue.Queue(maxsize=100)  # 限制队列大小防止内存溢出
result_dict = {}
result_lock = threading.Lock()
stop_event = threading.Event()

def md5(t):
    t = t.encode() if type(t) == str else t
    return hashlib.md5(t).hexdigest()

used_time_stamp = set()

def permission(fn):
    def func(*l, **k):
        global used_time_stamp
        
        time_stamp = float(request.headers.get('time-stamp', '0'))
        stamp = request.headers.get('stamp', '')
        now_time_stamp = time.time()
        
        cond1 = abs(time_stamp - now_time_stamp) < 2
        true_stamp = md5(password + str(time_stamp))
        cond2 = true_stamp == stamp
        used_time_stamp = {i for i in used_time_stamp if abs(now_time_stamp - i) < 5}
        cond3 = time_stamp not in used_time_stamp
        
        cond = cond1 & cond2 & cond3
        if cond:
            used_time_stamp.add(time_stamp)
            return fn(*l, **k)
        return {'error_msg': 'U have no permission'}, 401
    func.__name__ = fn.__name__
    return func

def handle_error(fn):
    def func(*l, **k):
        try:
            r = fn(*l, **k)
        except Exception as e:
            message = "{}: {}".format(e.__class__, e)
            r = jsonify({"error": message}), 400
        return r
    func.__name__ = fn.__name__
    return func

# 任务消费者线程
def task_consumer():
    while not stop_event.is_set():
        try:
            # 从队列获取任务，设置1秒超时防止永久阻塞
            task = task_queue.get(timeout=1.0)
            task_id, method, args, kwargs = task
            
            try:
                # 执行实际任务
                func = getattr(user, method)
                if isinstance(func, MethodType):
                    result = func(*args, **kwargs)
                else:
                    result = func
                
                # 存储结果
                with result_lock:
                    result_dict[task_id] = {"status": "completed", "result": result}
            
            except Exception as e:
                # 存储错误信息
                with result_lock:
                    result_dict[task_id] = {
                        "status": "error",
                        "error": f"{e.__class__.__name__}: {str(e)}"
                    }
            
            finally:
                # 标记任务完成
                task_queue.task_done()
        
        except queue.Empty:
            # 队列为空时继续等待
            continue

# 启动消费者线程
consumer_thread = threading.Thread(target=task_consumer, daemon=True)
consumer_thread.start()

@app.route("/<method>", methods=['get', 'post', 'put'])
@permission
@handle_error
def fn(method):
    data = request.get_json()
    args = data.get('args', [])
    kwargs = data.get('kwargs', {})
    
    # 生成唯一任务ID
    task_id = str(uuid.uuid4())
    
    # 检查队列是否已满
    if task_queue.full():
        return jsonify({
            "error": "Service busy",
            "message": "Task queue is full, please try again later"
        }), 503
    
    # 创建任务并加入队列
    task = (task_id, method, args, kwargs)
    task_queue.put(task)
    
    # 初始结果状态
    with result_lock:
        result_dict[task_id] = {"status": "queued", "position": task_queue.qsize()}
    
    return jsonify({"task_id": task_id, "status": "queued"}), 202

@app.route("/task_status/<task_id>", methods=['GET'])
def task_status(task_id):
    with result_lock:
        status_info = result_dict.get(task_id)
    
    if not status_info:
        return jsonify({"error": "Task not found"}), 404
    
    if status_info["status"] == "queued":
        # 获取队列中的位置
        return jsonify({
            "task_id": task_id,
            "status": "queued",
            "position": task_queue.qsize()
        })
    
    elif status_info["status"] == "completed":
        # 返回任务结果并清理
        result = status_info["result"]
        with result_lock:
            del result_dict[task_id]
        return jsonify({"task_id": task_id, "status": "completed", "result": result})
    
    elif status_info["status"] == "error":
        # 返回错误信息并清理
        error_msg = status_info["error"]
        with result_lock:
            del result_dict[task_id]
        return jsonify({
            "task_id": task_id,
            "status": "error",
            "error": error_msg
        }), 400

def cleanup_results():
    """定期清理过期的任务结果"""
    while not stop_event.is_set():
        time.sleep(60)  # 每分钟清理一次
        now = time.time()
        with result_lock:
            # 删除超过30分钟的任务结果
            keys_to_delete = [k for k, v in result_dict.items() 
                             if v.get("timestamp", 0) < now - 1800]
            for k in keys_to_delete:
                del result_dict[k]

# 启动清理线程
cleanup_thread = threading.Thread(target=cleanup_results, daemon=True)
cleanup_thread.start()

# 添加关闭钩子
@app.teardown_appcontext
def shutdown(exception=None):
    stop_event.set()
    consumer_thread.join(timeout=5.0)
    cleanup_thread.join(timeout=5.0)

if __name__ == '__main__':
    server = pywsgi.WSGIServer(('0.0.0.0', port), app)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        shutdown()
        server.stop()