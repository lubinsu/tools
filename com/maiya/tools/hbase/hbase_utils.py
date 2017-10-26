"""
hbase 连接
"""
import json
import random

import happybase
from happybase import Batch
from happybase import Connection
from happybase import ConnectionPool


def createNewConnection():
    hosts = {0: 'myd4', 1: 'myd5', 2: 'myd6'}
    connection = Connection(host=hosts[random.randint(1, 10) % 3], port=20550)
    return connection


def getTable(table_name, conn):
    return conn.table(table_name)


if __name__ == '__main__':

    _pool = None

    def get_pool():
        global _pool
        if _pool is None:
            try:
                _pool = happybase.ConnectionPool(size=1, host='myd3')
            except ConnectionRefusedError:
                _pool = happybase.ConnectionPool(size=1, host='myd4')

        return _pool


    with get_pool().connection() as connection:
        table = connection.table('my_busi_int_monitor')
        batch = table.batch(batch_size=10)
        batch.put('123', {b'f:cost': b'2', b'f:createtime': b'value2'})
        batch.send()
