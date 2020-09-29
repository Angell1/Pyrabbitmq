import pika
import threading
import random
import uuid


"""
Class:  
Parameters:
    Connectionsize:int类型，Rabbitmqpool池连接的最大数
    Channelsize:int类型，Rabbitmqpool池Channel的最大数
return:None
"""
# 单例保证唯一
class Rabbitmqpool:
    # 定义类属性
    __instance = None
    __lock = threading.Lock()

    def __init__(self, Connectionsize, Channelsize):
        self.maxConnectionsize = Connectionsize
        self.maxChannelsize = Channelsize
        self.nowConnectionsize = 0
        self.nowChannelsize = 0
        self.connectpool = {}
        self.channelpool = {}
        self.certdic = {}
    def __new__(cls, Connectionsize, Channelsize):
        if not cls.__instance:
            cls.__instance = object.__new__(cls)
        return cls.__instance
    """
    function:  获取一个空闲Channel或者新建一个Channel
    Parameters:

    return:
        channel：channel
        cname：连接名
    """

    def get_channel(self):
        try:
            self.__lock.acquire()
            cname = ""
            channel = None
            # 在已存在键中查找空闲Channel
            for connectionname in self.connectpool:
                if len(self.channelpool[connectionname]) != 0:
                    channel = self.channelpool[connectionname][-1]
                    cname = connectionname
                    self.channelpool[connectionname] = self.channelpool[connectionname][0:-1]
                    print("取出一个Channel")
                    break
            # 如果没有找到空闲Channel，canme为"",则新建一个Channel
            if cname == "":
                if self.nowChannelsize < self.maxChannelsize:
                    # 从连接池返回一个连接的名字
                    if len(self.connectpool) != 0:
                        cname = random.choice(list(self.connectpool))
                        # 根据名字拿到此连接，传入连接和Pool池创建Channel
                        CreateChannel(self.connectpool[cname], self)
                        # 得到一个新Channel
                        channel = self.channelpool[cname][-1]
                        self.channelpool[cname] = self.channelpool[cname][0:-1]
                        print("创建一个Channel")
                    # 如果没有连接，则新建连接与channel
                    else:
                        if len(self.certdic) != 0:
                            cert = random.choice(list(self.certdic))
                            cname = str(uuid.uuid4().int)
                            print("创建一个连接")
                            CreateConnection(str(self.certdic[cert]["rabbitmq_host"]), str(self.certdic[cert]["rabbitmq_port"]),
                                             str(self.certdic[cert]["rabbitmq_virtual_host"]),
                                             str(self.certdic[cert]["rabbitmq_user"]),
                                             str(self.certdic[cert]["rabbitmq_password"]), self, cname)
                            CreateChannel(self.connectpool[cname], self)
                            # 得到一个新Channel
                            channel = self.channelpool[cname][-1]
                            self.channelpool[cname] = self.channelpool[cname][0:-1]
                            print("创建一个Channel")
                        else:
                            print("无法创建Channel,无连接凭证,不能创建连接！")
                else:
                    print("无法创建Channel，超过限制")

        finally:
            self.__lock.release()
        return channel, cname

    def create_channel(self):
        try:
            self.__lock.acquire()
            if len(self.certdic) != 0:
                cert = random.choice(list(self.certdic))
                cname = str(uuid.uuid4().int)
                print("创建一个连接")
                CreateConnection(str(self.certdic[cert]["rabbitmq_host"]), str(self.certdic[cert]["rabbitmq_port"]),
                                 str(self.certdic[cert]["rabbitmq_virtual_host"]),
                                 str(self.certdic[cert]["rabbitmq_user"]),
                                 str(self.certdic[cert]["rabbitmq_password"]), self, cname)
                CreateChannel(self.connectpool[cname], self)
                # 得到一个新Channel
                channel = self.channelpool[cname][-1]
                self.channelpool[cname] = self.channelpool[cname][0:-1]
                print("创建一个Channel")
                return channel, cname
            else:
                print("无法创建Channel,无连接凭证,不能创建连接！")
            return None,""
        finally:
            self.__lock.release()

    def return_channel(self, channel, connectionname):
        try:
            self.__lock.acquire()
            self.channelpool[connectionname].append(channel)
        finally:
            self.__lock.release()

    def closepool(self):
        pass

    def delconnection(self, connectionname):
        try:
            self.__lock.acquire()
            if connectionname in self.connectpool:
                del self.connectpool[connectionname]
                self.nowConnectionsize = self.nowConnectionsize -1
                self.nowChannelsize  = self.nowChannelsize - len(self.channelpool[connectionname])
                del self.channelpool[connectionname]

        finally:
            self.__lock.release()

    def get_certtemplate(self):
        return {"rabbitmq_host": "", "rabbitmq_port": 5672, "rabbitmq_virtual_host": "", "rabbitmq_user": "",
                "rabbitmq_password": ""}

    def addcert(self,cert):
        self.certdic[cert["rabbitmq_host"]] = cert


# 连接可以自己创建
class CreateConnection:
    def __init__(self, rabbitmq_host, rabbitmq_port, rabbitmq_virtual_host, rabbitmq_user, rabbitmq_password,
                 Rabbitmqpool, Connectionname = str(uuid.uuid4().int), heartbeat=60):
        if Rabbitmqpool.nowConnectionsize < Rabbitmqpool.maxConnectionsize:
            if Connectionname not in Rabbitmqpool.connectpool:
                self.rabbitmq_user = str(rabbitmq_user)
                self.rabbitmq_password = str(rabbitmq_password)
                self.rabbitmq_host = rabbitmq_host
                self.rabbitmq_port = rabbitmq_port
                self.rabbitmq_virtual_host = rabbitmq_virtual_host
                self.connectionname = Connectionname
                print(self.rabbitmq_user,self.rabbitmq_password,self.rabbitmq_host,self.rabbitmq_port,self.rabbitmq_virtual_host,self.connectionname)
                credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
                try:
                    self.connection = pika.BlockingConnection(
                        pika.ConnectionParameters(
                            host=rabbitmq_host,
                            port=rabbitmq_port,
                            virtual_host=rabbitmq_virtual_host,
                            heartbeat=heartbeat,
                            credentials=credentials))
                    Rabbitmqpool.connectpool[Connectionname] = self
                    Rabbitmqpool.nowConnectionsize += 1
                    if self.connectionname not in Rabbitmqpool.channelpool:
                        Rabbitmqpool.channelpool[self.connectionname] = []
                    print("创建连接：", Connectionname)
                except Exception as e:
                    print("创建连接失败：", e)
            else:
                print("创建连接失败，此连接名已存在:", Connectionname)
        else:
            print("创建连接失败，连接池已满，无法创建连接池")

    def get_connection(self):
        return self.connection


class CreateChannel:
    def __init__(self, Connection, Rabbitmqpool):
        Rabbitmqpool.channelpool[Connection.connectionname].append(Connection.get_connection().channel())
        Rabbitmqpool.nowChannelsize += 1