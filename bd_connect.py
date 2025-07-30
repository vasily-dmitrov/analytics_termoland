import os
import pymysql
from sshtunnel import SSHTunnelForwarder
from functools import wraps
import socket

from dotenv import load_dotenv


load_dotenv()  # автоматически ищет .env в текущей папке

# НА ЦЕЛЕВОМ СЕРВЕРЕ ЛИ СКРИПТ
def is_on_target_host(target_host):
    try:
        # Получаем внешний IP текущей машины
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        # Сравниваем с целевым хостом
        return local_ip == target_host or target_host in ('localhost', '127.0.0.1')
    except Exception:
        return False


# ДЕКОРАТОР SSH ТОННЕЛЯ В СЛУЧАЕ ЕСЛИ СКРИПТ НЕ НА ЦЕЛЕВОМ СЕРВЕРЕ
def with_optional_ssh_tunnel(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        need_tunnel = not is_on_target_host(self.ssh_host)
        if need_tunnel:
            with SSHTunnelForwarder(
                (self.ssh_host, self.ssh_port),
                ssh_username=self.ssh_user,
                ssh_password=self.ssh_password,
                remote_bind_address=(self.db_host, self.db_port),
                local_bind_address=('127.0.0.1', self.local_port)
            ) as tunnel:
                self._tunnel = tunnel
                result = method(self, *args, **kwargs)
                self._tunnel = None
                return result
        else:
            return method(self, *args, **kwargs)
    return wrapper

#КЛАСС СОЕДИНЕНИЯ С БД
class MariaDBOverSSH:
    def __init__(self, ssh_host, ssh_port, ssh_user, ssh_password,
                 db_host, db_port, db_user, db_password, db_name, local_port=3307):
        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.ssh_user = ssh_user
        self.ssh_password = ssh_password
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.db_name = db_name
        self.local_port = local_port
        self._tunnel = None

    @with_optional_ssh_tunnel
    def query(self, sql, type='execute', data=None):
        '''
        :param sql:str  Text query formatting "SELECT * FROM total_clubs"
        :return: tuple
        '''
        # Если туннель открыт, подключаемся к локальному порту, иначе к реальному
        host = '127.0.0.1' if self._tunnel else self.db_host
        port = self.local_port if self._tunnel else self.db_port
        connection = pymysql.connect(
            host=host,
            port=port,
            user=self.db_user,
            password=self.db_password,
            database=self.db_name
        )
        try:
            with connection.cursor() as cursor:
                if type == 'insert':
                    cursor.executemany(sql, data)
                elif type == 'execute':
                    cursor.execute(sql)
                    result = cursor.fetchall()
                    return result
        finally:
            connection.close()


db = MariaDBOverSSH(
    ssh_host=os.getenv('SSH_HOST'),
    ssh_port=int(os.getenv('SSH_PORT')),
    ssh_user=os.getenv('SSH_USER'),
    ssh_password=os.getenv('SSH_PASS'),
    db_host=os.getenv('DB_HOST'),
    db_port=int(os.getenv('DB_PORT')),
    db_user=os.getenv('DB_USER'),
    db_password=os.getenv('DB_PASS'),
    db_name=os.getenv('DB_NAME'),
    local_port=int(os.getenv('LOCAL_PORT'))
)

a=0