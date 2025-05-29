from flask import Flask,request,jsonify
import flask
import easytrader
import time
import hashlib
from types import MethodType
from gevent import pywsgi
from queue import Queue
import threading
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('server.log'),
        logging.StreamHandler()
    ]
)

'''
config
'''
user=easytrader.use('htzq_client')#需要根据自身情况修改
user.connect('C:\htzq\海通证券金融终端独立下单2.0/xiadan.exe')#需要根据自身情况修改
password='your password'#填入你的通信密码，用于加密通信
port=5000#服务端口

# 创建消息队列
message_queue = Queue()
# 创建结果字典，用于存储处理结果
results = {}

app = Flask(__name__)

def md5(t):
    t=t.encode() if type(t)==str else t
    return hashlib.md5(t).hexdigest()

used_time_stamp=set()

def process_message():
    """消息处理线程函数"""
    while True:
        try:
            # 从队列获取消息
            message_id, method, args, kwargs = message_queue.get()
            logging.info(f"Processing message {message_id}: {method}")
            
            try:
                # 执行方法
                r = getattr(user, method)
                if isinstance(r, MethodType):
                    result = r(*args, **kwargs)
                else:
                    result = r
                
                # 存储结果
                results[message_id] = {
                    'status': 'success',
                    'result': result,
                    'timestamp': datetime.now().isoformat()
                }
            except Exception as e:
                # 存储错误信息
                results[message_id] = {
                    'status': 'error',
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }
            
            # 标记任务完成
            message_queue.task_done()
            
        except Exception as e:
            logging.error(f"Error in message processing: {str(e)}")
            time.sleep(1)  # 发生错误时等待一秒

# 启动消息处理线程
message_thread = threading.Thread(target=process_message, daemon=True)
message_thread.start()

def permission(fn):
    def func(*l,**k):
        global used_time_stamp
        
        time_stamp=float(request.headers.get('time-stamp','0'))
        stamp=request.headers.get('stamp','')
        now_time_stamp=time.time()
        
        cond1=abs(time_stamp-now_time_stamp)<200
        
        true_stamp=md5(password+str(time_stamp))
        cond2=true_stamp==stamp
        
        used_time_stamp={i for i in used_time_stamp if abs(now_time_stamp-i)<5}
        cond3=time_stamp not in used_time_stamp
        
        cond=cond1&cond2&cond3
        if cond:
            used_time_stamp.add(time_stamp)
            return fn(*l,**k)
        return {'error_msg':'U have no permission'},401
    func.__name__=fn.__name__
    return func

def handle_error(fn):
    def func(*l,**k):
        try:
            r=fn(*l,**k)
        except Exception as e:
            message = "{}: {}".format(e.__class__, e)
            r=jsonify({"error": message}), 400
        return r
    func.__name__=fn.__name__
    return func

@app.route("/<method>",methods=['get','post','put'])
@permission
@handle_error
def fn(method):
    data = request.get_json()
    args = data.get('args', list())
    kwargs = data.get('kwargs', dict())
    
    # 生成消息ID
    message_id = f"{method}_{int(time.time() * 1000)}"
    
    # 将消息加入队列
    message_queue.put((message_id, method, args, kwargs))
    
    # 等待消息处理完成
    max_wait_time = 30  # 最大等待时间（秒）
    start_time = time.time()
    
    while time.time() - start_time < max_wait_time:
        if message_id in results:
            result = results[message_id]
            # 清理结果
            del results[message_id]
            if result['status'] == 'success':
                return jsonify(result['result'])
            else:
                return jsonify({"error": result['error']}), 400
        time.sleep(0.1)
    
    return jsonify({"error": "Request timeout"}), 408

if __name__ == '__main__':
    server = pywsgi.WSGIServer(('0.0.0.0', port), app)
    server.serve_forever()
