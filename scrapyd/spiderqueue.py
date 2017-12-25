from zope.interface import implementer

from scrapyd.interfaces import ISpiderQueue
from scrapyd.sqlite import JsonSqlitePriorityQueue
from twisted.python import log


@implementer(ISpiderQueue)
class SqliteSpiderQueue(object):

    def __init__(self, database=None, table='spider_queue'):
        self.q = JsonSqlitePriorityQueue(database, table)

    def add(self, name, **spider_args):
        d = spider_args.copy()
        d['name'] = name
        priority = float(d.pop('priority', 0))
        self.q.put(d, priority)

    def pop(self):
        return self.q.pop()

    def count(self):
        return len(self.q)

    def list(self):
        return [x[0] for x in self.q]

    def remove(self, func):
        return self.q.remove(func)

    def clear(self):
        self.q.clear()


try:
    import  redis
except Exception as e:
    # raise e
    log.warning('not install redis module!!!')

@implementer(ISpiderQueue)
class RedisSpiderQueue(object):
    """store by redis"""

    def __init__(self, key='sort_name', db=0, password=None, host='localhost', port=6379, decode_responses=True):

        self.redis = redis.Redis(db=db, password=password, host=host, port=port, decode_responses=decode_responses)
        self.key = key

    # @property
    # def redis(self):
    #     return redis.Redis(host=self.host, port=self.port, decode_responses=self.decode_responses)

    def add(self, name, **spider_args):
      pass

    def pop(self):
        pass

    def count(self):
        # return self.redis.zcard(self.key)
        return 6000

    def list(self):
        pass

    def remove(self, func):
        pass

    def clear(self):
        pass

if __name__ == "__main__":
    print('='*80)
    rd = RedisSpiderQueue()
    r = rd.redis
    # print(rd.r.lpush('llist', 't1'))
    # r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    print(rd.count())
    # print(r.zadd('sort_name', 'test4', 20.0, 'test5', 10.0))
    # print(r.zcard('sort_name'))
    # print(r.zrange('sort_name', 0, 3))
    # print(r.zrangebyscore('sort_name', 0, 100, 2, -1))
    # print(r.zrevrangebyscore('sort_name', 1000, 0, 0, 5))