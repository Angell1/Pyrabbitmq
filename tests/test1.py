import pika
from bin import pyrabbitmq

rabbitmq_host = ""
rabbitmq_port = ""
rabbitmq_user = ""
rabbitmq_password = ""
rabbitmq_virtual_host = ""
credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
Pool = pyrabbitmq.Rabbitmqpool(3, 20)
cert = Pool.get_certtemplate()
cert['rabbitmq_host'] = rabbitmq_host
cert['rabbitmq_virtual_host'] = rabbitmq_virtual_host
cert['rabbitmq_user'] = rabbitmq_user
cert['rabbitmq_password'] = rabbitmq_password
cert['rabbitmq_port'] = rabbitmq_port
Pool.addcert(cert)

data = "hello word"

try:
    c, cname = Pool.get_channel()
    c.basic_publish(exchange='',
                    routing_key='队列名',
                    body=str(data),
                    )
    Pool.return_channel(c, cname)
except Exception as e:
    print("发送错误：", e)  # 链接过期
    Pool.delconnection(cname)  # channel过期时，删除此链接和此链接下的所有channel
    c, cname = Pool.create_channel()  # 创建一个新的链接和channel
    c.basic_publish(exchange='',
                    routing_key='队列名',
                    body=str(data),
                    )
    Pool.return_channel(c, cname)
