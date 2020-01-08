import socket
import time

# 创建socket对象
s = socket.socket()
# 将socket绑定到本机IP和端口
s.bind(('192.168.1.130', 9000))
# 服务端开始监听来自客户端的连接
s.listen()
while True:
    c, addr = s.accept()
    count = 0

    while True:
      c.send('{"project":"mobile","protocol":"Dindex/","companycode":"05780","model":"Dprotocol","response":"SucceedHeSNNNllo","response_time":0.03257,"status":0}\n'.encode('utf-8'))
      time.sleep(0.005)
      count += 1
      if count > 100000:
          # 关闭连接
          c.close()
          break
    time.sleep(1)
