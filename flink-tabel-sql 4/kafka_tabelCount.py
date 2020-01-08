# https://pypi.org/project/kafka-python/
import pickle
import time
import json
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
                         key_serializer=lambda k: pickle.dumps(k),
                         value_serializer=lambda v: pickle.dumps(v))
start_time = time.time()
for i in range(0, 10000):
    print('------{}---------'.format(i))
    producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),compression_type='gzip')
    producer.send('test', [{"response":"testSucceed","status":0},{"response":"testSucceed","status":0},{"response":"testSucceed","status":0},{"response":"testSucceed","status":0},{"response":"testSucceed","status":0}])
#    future = producer.send('test', key='num', value=i, partition=0)
# 将缓冲区的全部消息push到broker当中
producer.flush()
producer.close()

end_time = time.time()
time_counts = end_time - start_time
print(time_counts)