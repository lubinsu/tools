"""
redis 工具类，数据接入redis
"""
import redis


class RedisUtils:
    """
    Redis接口函数
    """

    def __init__(self, host='192.168.0.182', port=6379):
        self.__r__ = redis.StrictRedis(host=host, port=port, db=0)

    def set(self, key, value):
        self.__r__.set(key, value)

    def get(self, key):
        value = self.__r__.get(key)
        return value


if __name__ == '__main__':
    r = RedisUtils()
    trading_price = float(r.get('btctrade_trading_price_doge').decode())
    print(trading_price)

    r.set('btctrade_trading_price_doge', 0.01952)