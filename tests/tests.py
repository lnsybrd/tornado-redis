import unittest

from tornado import ioloop
from functools import partial
import redis.trace as trace
import redis.redis as redis
import logging
import time
import threading


logger = logging.getLogger('test')
logger.setLevel(logging.DEBUG)
redis.logger.setLevel(logging.DEBUG)

tracer = partial(trace.echo, logger)

class TestTornadoRedis(unittest.TestCase):

    @tracer
    def setUp(self):
        # Create a callback that expects an 'OK' as it's result
        self.expectok = partial(self.expect, 'OK')

        self.ioloop = ioloop.IOLoop.instance()
        self.db = redis.Redis()

        self.db.connect(self.stop)
        self.db.select(11, self.expectok())
        self.db.flushdb(self.expectok())

    @tracer
    def cleanup(self):
        self.db.flushdb(self.expectok(next=self.stop))

    @tracer
    def stop(self):
        self.ioloop.stop()

    @tracer
    def start(self):
        self.ioloop.start()

    def expect(self, expected_value=None, expected_error=None, next=None, assertFunc=None):
        """
            Return a function for use as a callback that handles asserting on an expected value.
            The next parameter defines a function to call if the callback succeeds.  This provides rudimentary
            method chaining.
        """

        @tracer
        def _expect(received_error, received_value):
            self.assertEqual(received_error, expected_error)

            if isinstance(received_value, list):
                received_value = sorted(received_value)
                expected_v = sorted(expected_value)
            else:
                expected_v = expected_value

            func = assertFunc or self.assertEqual
            func(received_value, expected_v)

            if next:
                next()

        return _expect



class TestRedisKeyCommands(TestTornadoRedis):
    '''
    Test the set of 'key' commands as defined by the redis docs at :http://redis.io/commands#generic
    '''

    @tracer
    def test_delete(self):
        #Delete a single key
        self.db.set('key1', 'value', self.expectok())
        self.db.delete('key1', self.expect(1, next=self.cleanup))
        self.start()

        #Delete a key that doesn't exist
        self.db.delete('key1', self.expect(0, next=self.cleanup))
        self.start()

        #Delete a list of keys
        self.db.set('key1', 'value', self.expectok())
        self.db.set('key2', 'value', self.expectok())
        self.db.set('key3', 'value', self.expectok())
        self.db.delete('key1', 'key2', 'key3', self.expect(3, next=self.cleanup))
        self.start()

    @tracer
    def test_keys(self):
        self.db.set('key1', 'value', self.expectok())
        self.db.set('key2', 'value', self.expectok())
        self.db.set('key3', 'value', self.expectok())
        self.db.keys('key*', self.expect(['key1', 'key2', 'key3'], next=self.cleanup))
        self.start()

    @tracer
    def test_rename(self):
        #Success
        self.db.set('key1', 'value', self.expectok())
        self.db.rename('key1', 'key2', self.expectok())
        self.db.get('key2', self.expect('value', next=self.cleanup))
        self.start()

        #Try to rename a key that doesn't exist
        self.db.rename('key1','key2', self.expect(None, 'ERR no such key', next=self.cleanup))
        self.start()

        #Try to rename a key to the same name
        self.db.set('key1','value', self.expectok())
        self.db.rename('key1','key1', self.expect(None, 'ERR source and destination objects are the same', next=self.cleanup))
        self.start()

    @tracer
    def test_type(self):
        self.db.set('key1', 'value', self.expectok())
        self.db.type('key1', self.expect('string', next=self.cleanup))
        self.start()

        self.db.lpush('key1', 'value', self.expect(1))
        self.db.type('key1', self.expect('list', next=self.cleanup))
        self.start()

        self.db.sadd('key1', 'value', self.expect(1))
        self.db.type('key1', self.expect('set', next=self.cleanup))
        self.start()

        self.db.zadd('key1', 0, 'value', self.expect(1))
        self.db.type('key1', self.expect('zset', next=self.cleanup))
        self.start()

        self.db.hset('key1', 'field1', 'value', self.expect(1))
        self.db.type('key1', self.expect('hash', next=self.cleanup))
        self.start()

        #Type a non existent key
        self.db.type('key1', self.expect('none', next=self.cleanup))
        self.start()

    @tracer
    def test_exists(self):
        self.db.set('key1', 'value', self.expectok())
        self.db.exists('key1', self.expect(1))
        self.db.delete('key1', self.expect(1))
        self.db.exists('key1', self.expect(0, next=self.cleanup))
        self.start()

    @tracer
    def test_move(self):
        self.db.set('key1', 'value', self.expectok())
        self.db.move('key1', 13, self.expect(1))
        self.db.select(13, self.expectok())
        self.db.exists('key1', self.expect(1))
        self.db.delete('key1', self.expect(1, next=self.cleanup))
        self.db.select(11, self.expectok())
        self.start()

    @tracer
    def test_renamenx(self):
        self.db.set('key1', 'value', self.expectok())
        self.db.renamenx('key1', 'key2', self.expect(1, next=self.cleanup))
        self.start()

        self.db.set('key1', 'value', self.expectok())
        self.db.set('key2', 'value', self.expectok())
        self.db.renamenx('key1', 'key2', self.expect(0, next=self.cleanup))
        self.start()

    @tracer
    def test_expire(self):
        self.db.set('key1', 'value', self.expectok())
        self.db.expire('key1', 2, self.expect(1))
        self.db.ttl('key1', self.expect(2, next=self.cleanup))
        self.start()

    @tracer
    def test_expireat(self):
        self.db.set('key1', 'value', self.expectok())
        self.db.expireat('key1', int(time.time() + 5), self.expect(1))
        self.db.ttl('key1', self.expect(5, next=self.cleanup))
        self.start()

    @tracer
    def test_randomkey(self):

        self.db.set('key1', 'value', self.expectok())
        self.db.set('key2', 'value', self.expectok())
        self.db.set('key3', 'value', self.expectok())
        self.db.randomkey(self.expect(['key1', 'key2', 'key3'], next=self.cleanup, assertFunc=self.assertIn))
        self.start()

    @tracer
    def test_sort(self): #TODO: Build test
        pass


class TestRedisStringCommands(TestTornadoRedis):
    '''
    Test the set of 'string' commands as defined by the redis docs at :http://redis.io/commands#string
    '''

    @tracer
    def test_set(self):
        self.db.set('key0', 'value0', self.expectok(next=self.cleanup))
        self.start()

    @tracer
    def test_setnx(self):
        self.db.setnx('key0', 'value0', self.expect(1, next=self.cleanup))
        self.start()

        self.db.setnx('key0', 'value0', self.expect(1))
        self.db.setnx('key0', 'value1', self.expect(0, next=self.cleanup))
        self.start()

    @tracer
    def test_mset(self):
        self.db.mset('key0', 'value0', 'key1', 'value1', 'key2', 'value2', self.expectok(next=self.cleanup))
        self.start()

    @tracer
    def test_msetnx(self):
        self.db.mset('key0', 'value0', 'key1', 'value1', 'key2', 'value2', self.expectok(next=self.cleanup))
        self.start()

        self.db.msetnx('key0', 'value0', 'key1', 'value1', 'key2', 'value2', self.expect(1))
        self.db.msetnx('key0', 'value0', 'key1', 'value1', 'key2', 'value2', self.expect(0, next=self.cleanup))
        self.start()

    @tracer
    def test_get(self):
        self.db.set('key0', 'value0', self.expectok())
        self.db.get('key0', self.expect('value0', next=self.cleanup))
        self.start()

    @tracer
    def test_mget(self):
        self.db.mset('key0', 'value0', 'key1', 'value1', 'key2', 'value2', self.expectok())
        self.db.mget('key0', 'key1', 'key2', self.expect(['value0', 'value1', 'value2'], next=self.cleanup))
        self.start()

    @tracer
    def test_decr(self):
        self.db.set('key',10, self.expectok())
        self.db.decr('key', self.expect(9))
        self.db.get('key', self.expect(9, next=self.cleanup))
        self.start()

    @tracer
    def test_decrby(self):
        self.db.set('key',10, self.expectok())
        self.db.decrby('key', 2, self.expect(8))
        self.db.get('key', self.expect(8, next=self.cleanup))
        self.start()

    @tracer
    def test_incr(self):
        self.db.set('key',10, self.expectok())
        self.db.incr('key', self.expect(11))
        self.db.get('key', self.expect(11, next=self.cleanup))
        self.start()

    @tracer
    def test_incrby(self):
        self.db.set('key',10, self.expectok())
        self.db.incrby('key', 2, self.expect(12))
        self.db.get('key', self.expect(12, next=self.cleanup))
        self.start()

    @tracer
    def test_append(self):
        self.db.set('key', 'value', self.expectok())
        self.db.append('key', '-more', self.expect(10))
        self.db.get('key', self.expect('value-more', next=self.cleanup))
        self.start()

    @tracer
    def test_getset(self):
        self.db.set('key', 'value0', self.expectok())
        self.db.getset('key', 'value1', self.expect('value0'))
        self.db.get('key', self.expect('value1', next=self.cleanup))
        self.start()

        self.db.getset('key', 'value1', self.expect(None, next=self.cleanup))
        self.start()

class TestRedisHashCommands(TestTornadoRedis):
    '''
    Test the set of 'hash' commands as defined by the redis docs at :http://redis.io/commands#hash
    '''

    @tracer
    def test_hset(self):
        self.db.hset('key','field','value', self.expect(1, next=self.cleanup))
        self.start()

        self.db.hset('key','field','value', self.expect(1))
        self.db.hset('key','field','value', self.expect(0, next=self.cleanup))
        self.start()

    @tracer
    def test_hmset(self):
        self.db.hmset('key','field0','value0','field1','value1','field2','value2', self.expectok( next=self.cleanup))
        self.start()

    @tracer
    def test_hsetnx(self):
        self.db.hsetnx('key','field','value', self.expect(1))
        self.db.hsetnx('key','field','value', self.expect(0, next=self.cleanup))
        self.start()

    @tracer
    def test_hget(self):
        self.db.hset('key','field','value', self.expect(1))
        self.db.hget('key','field', self.expect('value', next=self.cleanup))
        self.start()

    @tracer
    def test_hmget(self):
        self.db.hmset('key','field0','value0','field1','value1','field2','value2', self.expectok())
        self.db.hmget('key','field0','field1','field2', self.expect(['value0','value1','value2'], next=self.cleanup))
        self.start()

        self.db.hmset('key','field0','value0','field1','value1','field2','value2', self.expectok())
        self.db.hmget('key','field0','field1','field2','field3', self.expect(['value0','value1','value2',None], next=self.cleanup))
        self.start()

    @tracer
    def test_hdel(self):
        self.db.hmset('key','field0','value0','field1','value1','field2','value2', self.expectok())
        self.db.hdel('key','field0', self.expect(1))
        self.db.hget('key','field0', self.expect(None, next=self.cleanup))
        self.start()

        self.db.hdel('key','field0', self.expect(0))
        self.db.hget('key','field0', self.expect(None, next=self.cleanup))
        self.start()

    @tracer
    def test_hgetall(self):
        self.db.hmset('key','field0','value0','field1','value1','field2','value2', self.expectok())
        self.db.hgetall('key', self.expect(['field0','value0','field1','value1','field2','value2'], next=self.cleanup))
        self.start()

        self.db.hgetall('key', self.expect([None], next=self.cleanup))
        self.start()

    @tracer
    def test_hlen(self):
        self.db.hmset('key','field0','value0','field1','value1','field2','value2', self.expectok())
        self.db.hlen('key', self.expect(3, next=self.cleanup))
        self.start()

        self.db.hlen('key', self.expect(0, next=self.cleanup))
        self.start()

    @tracer
    def test_hexists(self):
        self.db.hset('key','field','value', self.expect(1))
        self.db.hexists('key','field', self.expect(1, next=self.cleanup))
        self.start()

        self.db.hset('key','field','value', self.expect(1))
        self.db.hexists('key','wrongfield', self.expect(0, next=self.cleanup))
        self.start()

        self.db.hexists('key','field', self.expect(0, next=self.cleanup))
        self.start()

    @tracer
    def test_hincrby(self):
        self.db.hset('key' ,'field' ,1 ,self.expect(1))
        self.db.hincrby('key' ,'field' ,2 ,self.expect(3))
        self.db.hget('key','field', self.expect(3))

        self.db.hincrby('key' ,'field' ,-2 ,self.expect(1))
        self.db.hget('key','field', self.expect(1, next=self.cleanup))

        self.start()

    @tracer
    def test_hkeys(self):
        self.db.hmset('key','field0','value0','field1','value1','field2','value2', self.expectok())
        self.db.hkeys('key', self.expect(['field0','field1','field2'], next=self.cleanup))
        self.start()

        self.db.hkeys('key', self.expect([None], next=self.cleanup))
        self.start()

    @tracer
    def test_hvals(self):
        self.db.hmset('key','field0','value0','field1','value1','field2','value2', self.expectok())
        self.db.hvals('key', self.expect(['value0','value1','value2'], next=self.cleanup))
        self.start()

        self.db.hvals('key', self.expect([None], next=self.cleanup))
        self.start()


class TestRedisListCommands(TestTornadoRedis):
    '''
    Test the set of 'list' commands as defined by the redis docs at :http://redis.io/commands#list
    '''

    @tracer
    def test_lpush(self):
        self.db.lpush('key','value0', self.expect(1, next=self.cleanup))
        self.start()

        self.db.lpush('key','value0', self.expect(1))
        self.db.lpush('key','value1', self.expect(2, next=self.cleanup))
        self.start()

    @tracer
    def test_rpush(self):
        self.db.rpush('key','value0', self.expect(1, next=self.cleanup))
        self.start()

        self.db.rpush('key','value0', self.expect(1))
        self.db.rpush('key','value1', self.expect(2, next=self.cleanup))
        self.start()

    @tracer
    def test_lpop(self):
        self.db.lpush('key','value0', self.expect(1))
        self.db.lpush('key','value1', self.expect(2))
        self.db.lpush('key','value2', self.expect(3))
        self.db.lpop('key',self.expect('value2', next=self.cleanup))
        self.start()

        self.db.rpush('key','value0', self.expect(1))
        self.db.rpush('key','value1', self.expect(2))
        self.db.rpush('key','value2', self.expect(3))
        self.db.lpop('key',self.expect('value0', next=self.cleanup))
        self.start()

    @tracer
    def test_rpop(self):
        self.db.rpush('key','value0', self.expect(1))
        self.db.rpush('key','value1', self.expect(2))
        self.db.rpush('key','value2', self.expect(3))
        self.db.rpop('key',self.expect('value2', next=self.cleanup))
        self.start()

        self.db.lpush('key','value0', self.expect(1))
        self.db.lpush('key','value1', self.expect(2))
        self.db.lpush('key','value2', self.expect(3))
        self.db.rpop('key',self.expect('value0', next=self.cleanup))
        self.start()

    @tracer
    def test_llen(self):
        self.db.rpush('key','value0', self.expect(1))
        self.db.rpush('key','value1', self.expect(2))
        self.db.rpush('key','value2', self.expect(3))
        self.db.llen('key',self.expect(3, next=self.cleanup))
        self.start()

    @tracer
    def test_lrem(self):
        self.db.rpush('key','value0', self.expect(1))
        self.db.rpush('key','value0', self.expect(2))
        self.db.rpush('key','value1', self.expect(3))
        self.db.lrem('key',2, 'value0', self.expect(2))
        self.db.lrange('key',0,-1,self.expect(['value1'], next=self.cleanup))
        self.start()

        self.db.rpush('key','value0', self.expect(1))
        self.db.rpush('key','value0', self.expect(2))
        self.db.rpush('key','value1', self.expect(3))
        self.db.lrem('key',-2, 'value0', self.expect(2))
        self.db.lrange('key',0,-1,self.expect(['value1'], next=self.cleanup))
        self.start()

        self.db.rpush('key','value0', self.expect(1))
        self.db.rpush('key','value1', self.expect(2))
        self.db.rpush('key','value1', self.expect(3))
        self.db.rpush('key','value0', self.expect(4))
        self.db.lrem('key',-1, 'value0', self.expect(1))
        self.db.lrange('key',0,-1,self.expect(['value0','value1','value1'], next=self.cleanup))
        self.start()

        self.db.rpush('key','value0', self.expect(1))
        self.db.rpush('key','value1', self.expect(2))
        self.db.rpush('key','value1', self.expect(3))
        self.db.rpush('key','value0', self.expect(4))
        self.db.lrem('key',1, 'value0', self.expect(1))
        self.db.lrange('key',0,-1,self.expect(['value1','value1','value0'], next=self.cleanup))
        self.start()

        self.db.rpush('key','value0', self.expect(1))
        self.db.rpush('key','value1', self.expect(2))
        self.db.rpush('key','value1', self.expect(3))
        self.db.rpush('key','value0', self.expect(4))
        self.db.lrem('key',0, 'value0', self.expect(2))
        self.db.lrange('key',0,-1,self.expect(['value1','value1'], next=self.cleanup))
        self.start()

    @tracer
    def test_lset(self):
        self.db.rpush('key','value0', self.expect(1))
        self.db.rpush('key','value1', self.expect(2))
        self.db.rpush('key','value1', self.expect(3))
        self.db.rpush('key','value0', self.expect(4))
        self.db.lset('key',0,'value1', self.expectok())
        self.db.lrange('key',0,-1,self.expect(['value1','value1','value1','value0'], next=self.cleanup))
        self.start()

        self.db.rpush('key','value0', self.expect(1))
        self.db.rpush('key','value1', self.expect(2))
        self.db.rpush('key','value1', self.expect(3))
        self.db.rpush('key','value0', self.expect(4))
        self.db.lset('key',2,'value0', self.expectok())
        self.db.lrange('key',0,-1,self.expect(['value0','value1','value0','value0'], next=self.cleanup))
        self.start()

        self.db.rpush('key','value0', self.expect(1))
        self.db.rpush('key','value1', self.expect(2))
        self.db.lset('key',2,'value1', self.expect(None, 'ERR index out of range', next=self.cleanup))
        self.start()

    @tracer
    def test_lindex(self):
        self.db.rpush('key','value0', self.expect(1))
        self.db.rpush('key','value1', self.expect(2))
        self.db.rpush('key','value2', self.expect(3))
        self.db.lindex('key',1,self.expect('value1',next=self.cleanup))
        self.start()

        self.db.rpush('key','value0', self.expect(1))
        self.db.rpush('key','value1', self.expect(2))
        self.db.lindex('key',2,self.expect(None,next=self.cleanup))
        self.start()

    @tracer
    def test_ltrim(self):
        self.db.rpush('key','value0', self.expect(1))
        self.db.rpush('key','value1', self.expect(2))
        self.db.rpush('key','value2', self.expect(3))
        self.db.rpush('key','value3', self.expect(4))
        self.db.ltrim('key',0,1,self.expectok())
        self.db.lrange('key',0,-1,self.expect(['value0','value1'], next=self.cleanup))
        self.start()

        self.db.rpush('key','value0', self.expect(1))
        self.db.rpush('key','value1', self.expect(2))
        self.db.rpush('key','value2', self.expect(3))
        self.db.rpush('key','value3', self.expect(4))
        self.db.ltrim('key',1,-1,self.expectok())
        self.db.lrange('key',0,-1,self.expect(['value1','value2','value3'], next=self.cleanup))
        self.start()

        self.db.rpush('key','value0', self.expect(1))
        self.db.rpush('key','value1', self.expect(2))
        self.db.rpush('key','value2', self.expect(3))
        self.db.rpush('key','value3', self.expect(4))
        self.db.ltrim('key',1,-2,self.expectok())
        self.db.lrange('key',0,-1,self.expect(['value1','value2'], next=self.cleanup))
        self.start()

        self.db.rpush('key','value0', self.expect(1))
        self.db.rpush('key','value1', self.expect(2))
        self.db.rpush('key','value2', self.expect(3))
        self.db.rpush('key','value3', self.expect(4))
        self.db.ltrim('key',3,0,self.expectok())
        self.db.lrange('key',0,-1,self.expect([None]))
        self.db.exists('key',self.expect(0, next=self.cleanup))
        self.start()

    @tracer
    def test_rpoplpush(self):
        self.db.rpush('key','value0', self.expect(1))
        self.db.rpush('key','value1', self.expect(2))
        self.db.rpush('key','value2', self.expect(3))
        self.db.rpoplpush('key', 'key1', self.expect('value2'))
        self.db.lrange('key',0,-1,self.expect(['value0','value1']))
        self.db.lrange('key1',0,-1,self.expect(['value2'], next=self.cleanup))
        self.start()

        self.db.rpush('key','value0', self.expect(1))
        self.db.rpush('key','value1', self.expect(2))
        self.db.rpush('key','value2', self.expect(3))
        self.db.rpush('key1','value0', self.expect(1))
        self.db.rpoplpush('key', 'key1', self.expect('value2'))
        self.db.lrange('key',0,-1,self.expect(['value0','value1']))
        self.db.lrange('key1',0,-1,self.expect(['value2','value0'], next=self.cleanup))
        self.start()

    @tracer
    def test_blpop(self):
        self.db.rpush('key1','key1.value0', self.expect(1))
        self.db.rpush('key1','key1.value1', self.expect(2))
        self.db.rpush('key2','key2.value0', self.expect(1))
        self.db.rpush('key2','key2.value1', self.expect(2))
        self.db.blpop('key0','key1','key2',0,self.expect(['key1','key1.value0'], next=self.cleanup))
        self.start()

        self.db.rpush('key2','key2.value0', self.expect(1))
        self.db.rpush('key2','key2.value1', self.expect(2))
        self.db.blpop('key0','key1','key2',0,self.expect(['key2','key2.value0'], next=self.cleanup))
        self.start()

        self.db.rpush('key1','key1.value0', self.expect(1))
        self.db.rpush('key1','key1.value1', self.expect(2))
        self.db.rpush('key2','key2.value0', self.expect(1))
        self.db.rpush('key2','key2.value1', self.expect(2))
        self.db.blpop('key0','key1','key2',0,self.expect(['key1','key1.value0']))
        self.db.blpop('key0','key1','key2',0,self.expect(['key1','key1.value1']))
        self.db.blpop('key0','key1','key2',0,self.expect(['key2','key2.value0'], next=self.cleanup))
        self.start()

        self.db.blpop('key0','key1','key2',1,self.expect([None], next=self.cleanup))
        self.start()


    @tracer
    def test_brpop(self):
        self.db.rpush('key1','key1.value0', self.expect(1))
        self.db.rpush('key1','key1.value1', self.expect(2))
        self.db.rpush('key2','key2.value0', self.expect(1))
        self.db.rpush('key2','key2.value1', self.expect(2))
        self.db.brpop('key0','key1','key2',0,self.expect(['key1','key1.value1'], next=self.cleanup))
        self.start()

        self.db.rpush('key2','key2.value0', self.expect(1))
        self.db.rpush('key2','key2.value1', self.expect(2))
        self.db.brpop('key0','key1','key2',0,self.expect(['key2','key2.value1'], next=self.cleanup))
        self.start()

        self.db.rpush('key1','key1.value0', self.expect(1))
        self.db.rpush('key1','key1.value1', self.expect(2))
        self.db.rpush('key2','key2.value0', self.expect(1))
        self.db.rpush('key2','key2.value1', self.expect(2))
        self.db.brpop('key0','key1','key2',0,self.expect(['key1','key1.value1']))
        self.db.brpop('key0','key1','key2',0,self.expect(['key1','key1.value0']))
        self.db.brpop('key0','key1','key2',0,self.expect(['key2','key2.value1'], next=self.cleanup))
        self.start()

        self.db.brpop('key0','key1','key2',1,self.expect([None], next=self.cleanup))
        self.start()

class TestRedisSetCommands(TestTornadoRedis):
    '''
    Test the set of 'set' commands as defined by the redis docs at :http://redis.io/commands#set
    '''
    @tracer
    def test_sadd(self):
        self.db.sadd('key', 'value0', self.expect(1))
        self.db.sadd('key', 'value1', self.expect(1))
        self.db.sadd('key', 'value0', self.expect(0, next=self.cleanup))
        self.start()

    @tracer
    def test_scard(self):
        self.db.sadd('key', 'value0', self.expect(1))
        self.db.sadd('key', 'value1', self.expect(1))
        self.db.scard('key', self.expect(2, next=self.cleanup))
        self.start()

    @tracer
    def test_sdiff(self):
        self.db.sadd('key0', 'value0', self.expect(1))
        self.db.sadd('key0', 'value1', self.expect(1))
        self.db.sadd('key0', 'value2', self.expect(1))
        self.db.sadd('key1', 'value0', self.expect(1))
        self.db.sadd('key1', 'value1', self.expect(1))
        self.db.sdiff('key0','key1', self.expect(['value2']))
        self.db.sdiff('key1','key0', self.expect([None], next=self.cleanup))
        self.start()

    @tracer
    def test_sdiffstore(self):
        self.db.sadd('key0', 'value0', self.expect(1))
        self.db.sadd('key0', 'value1', self.expect(1))
        self.db.sadd('key0', 'value2', self.expect(1))
        self.db.sadd('key0', 'value3', self.expect(1))
        self.db.sadd('key1', 'value0', self.expect(1))
        self.db.sadd('key1', 'value1', self.expect(1))
        self.db.sdiffstore('key2','key0','key1', self.expect(2))
        self.db.smembers('key2', self.expect(['value2','value3'], next=self.cleanup))
        self.start()

    @tracer
    def test_sinter(self):
        self.db.sadd('key0', 'value0', self.expect(1))
        self.db.sadd('key0', 'value1', self.expect(1))
        self.db.sadd('key0', 'value2', self.expect(1))
        self.db.sadd('key0', 'value3', self.expect(1))
        self.db.sadd('key1', 'value0', self.expect(1))
        self.db.sadd('key1', 'value1', self.expect(1))
        self.db.sinter('key0','key1', self.expect(['value0','value1'], next=self.cleanup))
        self.start()

        self.db.sadd('key0', 'value0', self.expect(1))
        self.db.sadd('key1', 'value0', self.expect(1))
        self.db.sinter('key0','key1','key2', self.expect([None], next=self.cleanup))
        self.start()

    @tracer
    def test_sinterstore(self):
        self.db.sadd('key0', 'value0', self.expect(1))
        self.db.sadd('key0', 'value1', self.expect(1))
        self.db.sadd('key0', 'value2', self.expect(1))
        self.db.sadd('key0', 'value3', self.expect(1))
        self.db.sadd('key1', 'value0', self.expect(1))
        self.db.sadd('key1', 'value1', self.expect(1))
        self.db.sinterstore('key2', 'key0','key1', self.expect(2))
        self.db.smembers('key2', self.expect(['value0','value1'], next=self.cleanup))
        self.start()

        self.db.sadd('key0', 'value0', self.expect(1))
        self.db.sadd('key1', 'value0', self.expect(1))
        self.db.sinterstore('key2','key0','key1', self.expect(1))
        self.db.smembers('key2', self.expect(['value0'], next=self.cleanup))
        self.start()

    @tracer
    def test_sismember(self):
        self.db.sadd('key0', 'value0', self.expect(1))
        self.db.sadd('key0', 'value1', self.expect(1))
        self.db.sismember('key0','value0',self.expect(1))
        self.db.sismember('key0','value2',self.expect(0, next=self.cleanup))
        self.start()

    @tracer
    def test_smembers(self):
        self.db.sadd('key0', 'value0', self.expect(1))
        self.db.sadd('key0', 'value1', self.expect(1))
        self.db.smembers('key0', self.expect(['value0','value1'], next=self.cleanup))
        self.start()

    @tracer
    def test_smove(self):
        self.db.sadd('key0', 'value0', self.expect(1))
        self.db.sadd('key0', 'value1', self.expect(1))
        self.db.smove('key0', 'key1', 'value0', self.expect(1, next=self.cleanup))
        self.start()

        self.db.sadd('key0', 'value0', self.expect(1))
        self.db.sadd('key0', 'value1', self.expect(1))
        self.db.sadd('key1', 'value0', self.expect(1))
        self.db.smove('key0', 'key1', 'value0', self.expect(1, next=self.cleanup))
        self.start()

        self.db.sadd('key0', 'value0', self.expect(1))
        self.db.sadd('key0', 'value1', self.expect(1))
        self.db.smove('key0', 'key1', 'value2', self.expect(0, next=self.cleanup))
        self.start()

        self.db.smove('key0', 'key1', 'value0', self.expect(0, next=self.cleanup))
        self.start()

    @tracer
    def test_spop(self):
        self.db.sadd('key0','value0',self.expect(1))
        self.db.spop('key0',self.expect('value0'))
        self.db.smembers('key0', self.expect([None], next=self.cleanup))
        self.start()

    @tracer
    def test_srandmember(self):
        self.db.sadd('key0','value0',self.expect(1))
        self.db.srandmember('key0',self.expect('value0'))
        self.db.smembers('key0', self.expect(['value0'], next=self.cleanup))
        self.start()

    @tracer
    def test_srem(self):
        self.db.sadd('key0', 'value0', self.expect(1))
        self.db.sadd('key0', 'value1', self.expect(1))
        self.db.srem('key0', 'value0', self.expect(1))
        self.db.srem('key0', 'value0', self.expect(0))
        self.db.smembers('key0', self.expect(['value1'], next=self.cleanup))
        self.start()

    @tracer
    def test_sunion(self):
        self.db.sadd('key0', 'value0', self.expect(1))
        self.db.sadd('key0', 'value1', self.expect(1))
        self.db.sadd('key1', 'value1', self.expect(1))
        self.db.sadd('key1', 'value2', self.expect(1))
        self.db.sunion('key0','key1', self.expect(['value0','value1','value2'], next=self.cleanup))
        self.start()

    @tracer
    def test_sunionstore(self):
        self.db.sadd('key0', 'value0', self.expect(1))
        self.db.sadd('key0', 'value1', self.expect(1))
        self.db.sadd('key1', 'value1', self.expect(1))
        self.db.sadd('key1', 'value2', self.expect(1))
        self.db.sunionstore('key2', 'key0','key1', self.expect(3))
        self.db.smembers('key2',self.expect(['value0','value1','value2'], next=self.cleanup))
        self.start()

class TestRedisSortedSetCommands(TestTornadoRedis):
    '''
    Test the set of 'zset' commands as defined by the redis docs at :http://redis.io/commands#zset
    '''

    @tracer
    def test_zadd(self):
        self.db.zadd('key0',0,'value0', self.expect(1))
        self.db.zadd('key0',10,'value0', self.expect(0, next=self.cleanup))
        self.start()

    @tracer
    def test_zcard(self):
        self.db.zadd('key0',0,'value0', self.expect(1))
        self.db.zadd('key0',1,'value1', self.expect(1))
        self.db.zadd('key0',2,'value2', self.expect(1))
        self.db.zcard('key0', self.expect(3, next=self.cleanup))
        self.start()

    @tracer
    def test_zcount(self):
        self.db.zadd('key0',0,'value0', self.expect(1))
        self.db.zadd('key0',1,'value1', self.expect(1))
        self.db.zadd('key0',2,'value2', self.expect(1))
        self.db.zadd('key0',3,'value3', self.expect(1))
        self.db.zcount('key0', '-inf', '+inf', self.expect(4))
        self.db.zcount('key0', 2, '+inf', self.expect(2))
        self.db.zcount('key0', '(2', '+inf', self.expect(1))
        self.db.zcount('key0', '1', '(2', self.expect(1))
        self.db.zcount('key0', '1', '2', self.expect(2))
        self.db.zcount('key0', '(1', '(2', self.expect(0, next=self.cleanup))
        self.start()

    @tracer
    def test_zincrby(self):
        self.db.zadd('key0',5,'value0', self.expect(1))
        self.db.zincrby('key0',10, 'value0', self.expect(15))
        self.db.zincrby('key0',-5, 'value0', self.expect(10))
        self.db.zincrby('key1',5, 'value0', self.expect(5))
        self.db.zincrby('key1',15, 'value0', self.expect(20, next=self.cleanup))
        self.start()

    @tracer
    def test_zinterstore(self):
        self.db.zadd('key0',1,'value1', self.expect(1))
        self.db.zadd('key0',2,'value2', self.expect(1))
        self.db.zadd('key0',3,'value3', self.expect(1))
        self.db.zadd('key0',4,'value4', self.expect(1))
        self.db.zadd('key1',3,'value3', self.expect(1))
        self.db.zadd('key1',4,'value4', self.expect(1))
        self.db.zadd('key1',5,'value5', self.expect(1))
        self.db.zadd('key1',6,'value6', self.expect(1))
        self.db.zinterstore('out',2,'key0','key1','WEIGHTS',2,3,self.expect(2))
        self.db.zrange('out',0, -1, 'WITHSCORES', self.expect(['value3',15, 'value4',20]))
        self.db.zinterstore('out',2,'key0','key1',self.expect(2))
        self.db.zrange('out',0, -1, 'WITHSCORES', self.expect(['value3',6, 'value4',8]))
        self.db.zinterstore('out',2,'key0','key1','WEIGHTS',2,3,'AGGREGATE','MIN',self.expect(2))
        self.db.zrange('out',0, -1, 'WITHSCORES', self.expect(['value3',6, 'value4',8]))
        self.db.zinterstore('out',2,'key0','key1','WEIGHTS',2,3,'AGGREGATE','MAX',self.expect(2))
        self.db.zrange('out',0, -1, 'WITHSCORES', self.expect(['value3',9, 'value4',12], next=self.cleanup))
        self.start()

    @tracer
    def test_zrange(self):
        self.db.zadd('key0',1,'value1', self.expect(1))
        self.db.zadd('key0',2,'value2', self.expect(1))
        self.db.zadd('key0',3,'value3', self.expect(1))
        self.db.zadd('key0',4,'value4', self.expect(1))
        self.db.zrange('key0', 0, -1, self.expect(['value1','value2','value3','value4']))
        self.db.zrange('key0',-2,-1, self.expect(['value3','value4']))
        self.db.zrange('key0',0,-1,'WITHSCORES', self.expect(['value1',1,'value2',2,'value3',3,'value4',4], next=self.cleanup))
        self.start()

    @tracer
    def test_zrangebyscore(self):
        self.db.zadd('key0',1,'value1', self.expect(1))
        self.db.zadd('key0',2,'value2', self.expect(1))
        self.db.zadd('key0',3,'value3', self.expect(1))
        self.db.zadd('key0',4,'value4', self.expect(1))
        self.db.zrangebyscore('key0', 1,3,self.expect(['value1','value2','value3']))
        self.db.zrangebyscore('key0', '(1',3,self.expect(['value2','value3']))
        self.db.zrangebyscore('key0', '(1','(3',self.expect(['value2']))
        self.db.zrangebyscore('key0', '-inf','(3',self.expect(['value1','value2'], next=self.cleanup))
        self.start()

    @tracer
    def test_zrank(self):
        self.db.zadd('key0',1,'value1', self.expect(1))
        self.db.zadd('key0',2,'value2', self.expect(1))
        self.db.zadd('key0',3,'value3', self.expect(1))
        self.db.zadd('key0',4,'value4', self.expect(1))
        self.db.zrank('key0', 'value1', self.expect(0))
        self.db.zrank('key0', 'value2', self.expect(1))
        self.db.zrank('key0', 'value5', self.expect(None))
        self.db.zrank('key1', 'value0', self.expect(None, next=self.cleanup))
        self.start()

    @tracer
    def test_zrem(self):
        self.db.zadd('key0',1,'value1', self.expect(1))
        self.db.zadd('key0',2,'value2', self.expect(1))
        self.db.zadd('key0',3,'value3', self.expect(1))
        self.db.zadd('key0',4,'value4', self.expect(1))
        self.db.zrem('key0','value2', self.expect(1))
        self.db.zrem('key0','value5', self.expect(0))
        self.db.zrange('key0',0,-1, self.expect(['value1','value3','value4'], next=self.cleanup))
        self.start()

    @tracer
    def test_zremrangebyrank(self):
        self.db.zadd('key0',1,'value1', self.expect(1))
        self.db.zadd('key0',2,'value2', self.expect(1))
        self.db.zadd('key0',3,'value3', self.expect(1))
        self.db.zadd('key0',4,'value4', self.expect(1))
        self.db.zremrangebyrank('key0',0,1, self.expect(2))
        self.db.zrange('key0',0,-1, self.expect(['value3','value4']))
        self.db.zadd('key0',1,'value1', self.expect(1))
        self.db.zadd('key0',2,'value2', self.expect(1))
        self.db.zremrangebyrank('key0',-2,-1, self.expect(2))
        self.db.zrange('key0',0,-1, self.expect(['value1','value2'], next=self.cleanup))
        self.start()

    @tracer
    def test_zremrangebyscore(self):
        self.db.zadd('key0',1,'value1', self.expect(1))
        self.db.zadd('key0',2,'value2', self.expect(1))
        self.db.zadd('key0',3,'value3', self.expect(1))
        self.db.zadd('key0',4,'value4', self.expect(1))
        self.db.zremrangebyscore('key0',3,4, self.expect(2))
        self.db.zrange('key0',0,-1, self.expect(['value1','value2']))
        self.db.zadd('key0',3,'value3', self.expect(1))
        self.db.zadd('key0',4,'value4', self.expect(1))
        self.db.zremrangebyscore('key0',3,'+inf', self.expect(2))
        self.db.zrange('key0',0,-1, self.expect(['value1','value2'], next=self.cleanup))
        self.start()

    @tracer
    def test_zrevrange(self):
        self.db.zadd('key0',1,'value1', self.expect(1))
        self.db.zadd('key0',2,'value2', self.expect(1))
        self.db.zadd('key0',3,'value3', self.expect(1))
        self.db.zadd('key0',4,'value4', self.expect(1))
        self.db.zrange('key0', 0, -1, self.expect(['value4','value3','value2','value1']))
        self.db.zrange('key0',-2,-1, self.expect(['value3','value4']))
        self.db.zrange('key0',0,-1,'WITHSCORES', self.expect(['value4',4,'value3',3,'value2',2,'value1',1], next=self.cleanup))
        self.start()

    @tracer
    def test_zrevrank(self):
        self.db.zadd('key0',1,'value1', self.expect(1))
        self.db.zadd('key0',2,'value2', self.expect(1))
        self.db.zadd('key0',3,'value3', self.expect(1))
        self.db.zadd('key0',4,'value4', self.expect(1))
        self.db.zrevrank('key0', 'value1', self.expect(3))
        self.db.zrevrank('key0', 'value2', self.expect(2))
        self.db.zrevrank('key0', 'value5', self.expect(None))
        self.db.zrevrank('key1', 'value0', self.expect(None, next=self.cleanup))
        self.start()

    @tracer
    def test_zscore(self):
        self.db.zadd('key0',1,'value1', self.expect(1))
        self.db.zadd('key0',2,'value2', self.expect(1))
        self.db.zadd('key0',3,'value3', self.expect(1))
        self.db.zadd('key0',4,'value4', self.expect(1))
        self.db.zscore('key0','value3', self.expect(3, next=self.cleanup))
        self.start()

    @tracer
    def test_zunionstore(self):
        self.db.zadd('key0',1,'value1', self.expect(1))
        self.db.zadd('key0',2,'value2', self.expect(1))
        self.db.zadd('key0',3,'value3', self.expect(1))
        self.db.zadd('key0',4,'value4', self.expect(1))
        self.db.zadd('key1',3,'value3', self.expect(1))
        self.db.zadd('key1',4,'value4', self.expect(1))
        self.db.zadd('key1',5,'value5', self.expect(1))
        self.db.zadd('key1',6,'value6', self.expect(1))
        self.db.zunionstore('out',2,'key0','key1','WEIGHTS',2,3,self.expect(6))
        self.db.zrange('out',0, -1, 'WITHSCORES', self.expect(['value1',2,'value2',4,'value3',15, 'value4',20,'value5',15,'value6',18]))
        self.db.zunionstore('out',2,'key0','key1',self.expect(6))
        self.db.zrange('out',0, -1, 'WITHSCORES', self.expect(['value1',1,'value2',2,'value3',6, 'value4',8,'value5',5,'value6',6]))
        self.db.zunionstore('out',2,'key0','key1','WEIGHTS',2,3,'AGGREGATE','MIN',self.expect(6))
        self.db.zrange('out',0, -1, 'WITHSCORES', self.expect(['value1',2,'value2',4,'value3',6, 'value4',8,'value5',15,'value6',18]))
        self.db.zunionstore('out',2,'key0','key1','WEIGHTS',2,3,'AGGREGATE','MAX',self.expect(6))
        self.db.zrange('out',0, -1, 'WITHSCORES', self.expect(['value1',2,'value2',4,'value3',9, 'value4',12,'value5',15,'value6',18], next=self.cleanup))
        self.start()

class TestRedisConnectionCommands(TestTornadoRedis):
    '''
    Test the set of 'connection' commands as defined by the redis docs at :http://redis.io/commands#connection
    '''
    @tracer
    def test_ping(self):
        self.db.ping(self.expect('PONG', next=self.cleanup))
        self.start()

    @tracer
    def test_echo(self):
        self.db.echo('Hello World', self.expect('Hello World', next=self.cleanup))
        self.start()

    #@tracer
    #def test_quit(self):
    #    def expect_close(value, error):
    #        logger.debug('EXPECT_CLOSE CALLED')
    #        self.stop()

    #    self.db.quit(expect_close)
    #    self.start()

class TestRedisServerCommands(TestTornadoRedis):
    '''
    Test the set of 'server' commands as defined by the redis docs at :http://redis.io/commands#server
    '''

    @tracer
    def test_dbsize(self):
        self.db.dbsize(self.expect(0))
        self.db.set('key0','value0', self.expectok())
        self.db.set('key1','value1', self.expectok())
        self.db.set('key2','value2', self.expectok())
        self.db.dbsize(self.expect(3, next=self.cleanup))

        self.start()

    @tracer
    def test_info(self):

        def info_callback(err, val):
            logger.debug(err)
            logger.debug(val)
            self.assertTrue(True)
            self.cleanup()

        self.db.info(info_callback)
        self.start()

class TestRedisPubSubCommands(TestTornadoRedis):
    '''
    Test the set of 'server' commands as defined by the redis docs at :http://redis.io/commands#server
    '''

    @tracer
    def publish_thread_cb(self, err, val):
        logger.debug('published message to %d subscribers'%val)

    @tracer
    def publish_thread(self, channel):
        self.publish_db = redis.Redis()
        self.publish_db.connect()
        self.publish_db.publish(channel,'Test Message', self.publish_thread_cb)

    @tracer
    def test_subscribe(self):
        def on_message(c, msg):
            logger.debug('msg:%s'%msg)

            if msg[0] == 'subscribe':
                logger.debug('successfully subscribed')

                threading.Thread(target=partial(self.publish_thread, 'test')).start()

            elif msg[0] == 'message':
                self.assertEqual('Test Message', msg[2])
                self.db.unsubscribe(c)

            elif msg[0] == 'unsubscribe':
                self.cleanup()


        self.db.subscribe('test', partial(on_message,'test'))
        self.start()

    @tracer
    def test_psubscribe(self):
        def on_message(c, msg):
            logger.debug('msg:%s'%msg)

            if msg[0] == 'psubscribe':
                logger.debug('successfully subscribed')

                threading.Thread(target=partial(self.publish_thread, 'test.test')).start()

            elif msg[0] == 'pmessage':
                self.assertEqual('Test Message', msg[3])
                self.db.punsubscribe(c)

            elif msg[0] == 'punsubscribe':
                self.cleanup()

        self.db.psubscribe('test.*', partial(on_message, 'test.*'))
        self.start()

if __name__ == '__main__':
    suite = unittest.TestSuite()
    #suite.addTest(TestRedisKeyCommands('test_randomkey'))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestRedisKeyCommands))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestRedisHashCommands))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestRedisStringCommands))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestRedisSetCommands))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestRedisSortedSetCommands))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestRedisListCommands))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestRedisServerCommands))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestRedisPubSubCommands))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestRedisConnectionCommands))

    unittest.TextTestRunner().run(suite)
