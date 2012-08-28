#!/usr/bin/python2 

from tornado import iostream
import socket
import sys
import logging
import trace
from functools import partial

logging.basicConfig()

logger = logging.getLogger('redis')
logger.setLevel(logging.DEBUG)

tracer = partial(trace.echo, logger)

class enum(object):
    def __init__(self, *args):
        for name in list(args):
            setattr(self, name, name)

#Reply types
ReplyType = enum('MULTI_BULK','BULK','STATUS','INTEGER','SUBSCRIBE')

class Redis(object):


    def __init__(self, host='localhost', port=6379, db=0):

        self._host = host
        self._port = port
        self._db = db

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)

        self.STATUS_REPLY = (ReplyType.STATUS, self._handle_status_reply)
        self.INTEGER_REPLY = (ReplyType.INTEGER, self._handle_integer_reply)
        self.BULK_REPLY = (ReplyType.BULK, self._handle_bulk_reply)
        self.MULTI_BULK_REPLY = (ReplyType.MULTI_BULK, self._handle_multi_bulk_reply)
        self.SUBSCRIBE_REPLY = (ReplyType.SUBSCRIBE, self._handle_multi_bulk_reply)

        self._cmd_map = {
            #Connection commands
            'SELECT': self.STATUS_REPLY,
            'ECHO': self.BULK_REPLY,
            'PING': self.STATUS_REPLY,
            'QUIT': self.STATUS_REPLY,
            'AUTH': self.STATUS_REPLY,

            #Server commands
            'BGREWRITEAOF': self.STATUS_REPLY,
            'DBSIZE': self.INTEGER_REPLY,
            'INFO': self.BULK_REPLY,
            'SLAVEOF': self.STATUS_REPLY,
            'BGSAVE': self.STATUS_REPLY,
            'SAVE': self.STATUS_REPLY,
            'LASTSAVE': self.INTEGER_REPLY,
            'CONFIG GET': self.BULK_REPLY,
            'CONFIG SET': self.BULK_REPLY,
            'CONFIG RESETSTAT': self.STATUS_REPLY,
            'FLUSHALL': self.STATUS_REPLY,
            'FLUSHDB': self.STATUS_REPLY,
            'SHUTDOWN': self.STATUS_REPLY,

            #Sorted set commands
            'ZADD': self.INTEGER_REPLY,
            'ZINTERSTORE': self.INTEGER_REPLY,
            'ZUNIONSTORE': self.INTEGER_REPLY,
            'ZREM': self.INTEGER_REPLY,
            #'ZREVRANGEBYSCORE': self.MULTI_BULK_REPLY, #Redis > 2.1
            'ZCARD': self.INTEGER_REPLY,
            'ZRANGE': self.MULTI_BULK_REPLY,
            'ZREVRANGE': self.MULTI_BULK_REPLY,
            'ZREMRANGEBYRANK': self.INTEGER_REPLY,
            'ZCOUNT': self.INTEGER_REPLY,
            'ZRANGEBYSCORE': self.MULTI_BULK_REPLY,
            'ZREMRANGEBYSCORE': self.INTEGER_REPLY,
            'ZSCORE': self.BULK_REPLY,
            'ZINCRBY': self.BULK_REPLY,
            'ZREVRANK': self.INTEGER_REPLY,
            'ZRANK': self.INTEGER_REPLY,


            #Set commands
            'SADD': self.INTEGER_REPLY,
            'SMOVE': self.INTEGER_REPLY,
            'SCARD': self.INTEGER_REPLY,
            'SREM': self.INTEGER_REPLY,
            'SINTERSTORE': self.INTEGER_REPLY,
            'SUNIONSTORE': self.INTEGER_REPLY,
            'SDIFFSTORE': self.INTEGER_REPLY,
            'SISMEMBER': self.INTEGER_REPLY,
            'SPOP': self.BULK_REPLY,
            'SRANDMEMBER': self.BULK_REPLY,
            'SINTER': self.MULTI_BULK_REPLY,
            'SUNION': self.MULTI_BULK_REPLY,
            'SDIFF': self.MULTI_BULK_REPLY,
            'SMEMBERS': self.MULTI_BULK_REPLY,

            #List commands
            'BLPOP': self.MULTI_BULK_REPLY,
            'BRPOP': self.MULTI_BULK_REPLY,
            'LRANGE': self.MULTI_BULK_REPLY,
            'LLEN': self.INTEGER_REPLY,
            'LREM': self.INTEGER_REPLY,
            'RPUSH': self.INTEGER_REPLY,
            #'RPUSHX': self.INTEGER_REPLY, #Redis > 2.1
            'LPUSH': self.INTEGER_REPLY,
            #'LPUSHX': self.INTEGER_REPLY, #Redis > 2.1
            'LSET': self.STATUS_REPLY,
            'LTRIM': self.STATUS_REPLY,
            'RPOP': self.BULK_REPLY,
            'LPOP': self.BULK_REPLY,
            'LINDEX': self.BULK_REPLY,
            #'LINSERT': self.INTEGER_REPLY, #Redis > 2.1
            #'BRPOPLPUSH': self.BULK_REPLY, #Redis > 2.1
            'RPOPLPUSH': self.BULK_REPLY,

            #Hash commands
            'HDEL': self.INTEGER_REPLY,
            'HLEN': self.INTEGER_REPLY,
            'HSET': self.INTEGER_REPLY,
            'HGET': self.BULK_REPLY,
            'HMGET': self.MULTI_BULK_REPLY,
            'HKEYS': self.MULTI_BULK_REPLY,
            'HVALS': self.MULTI_BULK_REPLY,
            'HMSET': self.STATUS_REPLY,
            'HSETNX': self.INTEGER_REPLY,
            'HEXISTS': self.INTEGER_REPLY,
            'HINCRBY': self.INTEGER_REPLY,
            'HGETALL': self.MULTI_BULK_REPLY,

            #String commands
            'SET': self.STATUS_REPLY,
            'SETNX': self.INTEGER_REPLY,
            'SETEX': self.INTEGER_REPLY,
            'MSET': self.STATUS_REPLY,
            'MSETNX': self.INTEGER_REPLY,
            'APPEND': self.INTEGER_REPLY,
            #'GETRANGE': self.BULK_REPLY, #Redis > 2.1
            #'SETRANGE': self.INTEGER_REPLY, #Redis > 2.1
            'DECR': self.INTEGER_REPLY,
            'DECRBY': self.INTEGER_REPLY,
            'INCR': self.INTEGER_REPLY,
            'INCRBY': self.INTEGER_REPLY,
            'GET': self.BULK_REPLY,
            'GETSET': self.BULK_REPLY,
            #'STRLEN': self.INTEGER_REPLY, #Redis > 2.1
            'MGET': self.MULTI_BULK_REPLY,
            #'SETBIT': self.INTEGER_REPLY, #Redis > 2.1
            #'GETBIT': self.INTEGER_REPLY, #Redis > 2.1

            #Key commands
            'DEL': self.INTEGER_REPLY,
            'KEYS': self.MULTI_BULK_REPLY,
            'RENAME': self.STATUS_REPLY,
            'TYPE': self.STATUS_REPLY,
            'EXISTS': self.INTEGER_REPLY,
            'MOVE': self.INTEGER_REPLY,
            'RENAMENX': self.INTEGER_REPLY,
            'EXPIRE': self.INTEGER_REPLY,
            #'PERSIST': self.INTEGER_REPLY, #Redis > 2.1
            'SORT': self.MULTI_BULK_REPLY,
            'EXPIREAT': self.INTEGER_REPLY,
            'RANDOMKEY': self.BULK_REPLY,
            'TTL': self.INTEGER_REPLY,

            #PubSub commands
            'PUBLISH': self.INTEGER_REPLY,
        }
        self._build_cmds()

        #Add these after we build the functions because these are handled differently
        self._cmd_map['SUBSCRIBE'] = self.SUBSCRIBE_REPLY
        self._cmd_map['PSUBSCRIBE'] = self.SUBSCRIBE_REPLY
        self._cmd_map['UNSUBSCRIBE'] = self.SUBSCRIBE_REPLY
        self._cmd_map['PUNSUBSCRIBE'] = self.SUBSCRIBE_REPLY

        #A map of subscriptions to callbacks
        self._subscriptions = {}
        self._subscribed = False

        self._clear_state()

        self._cmd_queue = []


    @tracer
    def connect(self, close_callback=None):
        try:
            self._socket.connect((self._host, self._port))
        except socket.error, e:
            raise e

        self._stream = iostream.IOStream(self._socket)

        if close_callback:
            self._stream.set_close_callback(close_callback)

    @tracer
    def disconnect(self):
        self._stream.close()

    @tracer
    def _notify_subscribers(self, channel, msg):
        if channel in self._subscriptions:
            callbacks = self._subscriptions.get(channel, [])
            for callback in callbacks:
                callback(msg)

    @tracer
    def _subscribe_callback(self, error, msg):
        if msg:
            msg_type = msg[0]
            channel = msg[1]

            logger.debug('msg_type: %s'%msg_type)
            logger.debug('msg:%s'%msg)

            if msg_type == 'subscribe' or msg_type == 'psubscribe':
                logger.debug('successfully subscribed to: %s'%channel)
                self._subscribed = True

                self._notify_subscribers(channel, msg)

            elif msg_type == 'unsubscribe' or msg_type == 'punsubscribe':
                logger.debug('successfuly unsubscribed from: %s'%channel)

                if len(self._subscriptions) -1 == 0:
                    logger.debug('mo more subscriptions, allowing regular commands again')
                    #Unset the flag before notification because listeners may want to execute commands in their callback 
                    self._subscribed = False

                #Notify
                self._notify_subscribers(channel, msg)

                #Delete this after so we can notify subscribers of the unsubscription
                del self._subscriptions[channel]

            elif msg_type == 'message' or msg_type == 'pmessage':
                self._notify_subscribers(channel, msg)

    @tracer
    def subscribe(self, channel, onmessage):
        if channel in self._subscriptions: 
            #Just a new callback becuase we're already subscribed to the channel
            self._subscriptions[channel].append(onmessage)
        else:
            self._subscriptions[channel] = [onmessage]
            self._queue_command('SUBSCRIBE',channel, self._subscribe_callback) 

    @tracer
    def psubscribe(self, channel, onmessage):
        if channel in self._subscriptions: 
            #Just a new callback becuase we're already subscribed to the channel
            self._subscriptions[channel].append(onmessage)
        else:
            self._subscriptions[channel] = [onmessage]
            self._queue_command('PSUBSCRIBE',channel, self._subscribe_callback) 

    @tracer
    def unsubscribe(self, channel):
        if channel in self._subscriptions: 
            self._queue_command('UNSUBSCRIBE',channel, self._subscribe_callback) 

    @tracer
    def punsubscribe(self, channel):
        if channel in self._subscriptions: 
            self._queue_command('PUNSUBSCRIBE',channel, self._subscribe_callback) 

    @tracer
    def _queue_command(self, cmd, *args):
        arglist = list(args)
        callback = None

        if hasattr(arglist[-1], '__call__'):
            logger.debug('last argument is a function, using as callback')
            callback = arglist.pop()

        if cmd == 'SUBSCRIBE' or cmd == 'UNSUBSCRIBE' or cmd == 'PSUBSCRIBE' or cmd == 'PUNSUBSCRIBE':
            self._cmd_queue.append((cmd, arglist, callback))

            #Only send this if we are the only comamnd queued up, otherwise wait our turn
            if len(self._cmd_queue) == 1:
                self._send_next()

        elif not self._subscribed:
            logger.debug('appending %s to cmd_queue'%cmd)

            self._cmd_queue.append((cmd, arglist, callback))
            if not self._cur_cmd:
                self._send_next()

        else:
            self._execute_callback('ERR In publish subscribe mode', None)

    @tracer
    def _send_next(self):
        if len(self._cmd_queue) == 0:
            logger.debug('cmd queue is empty')
        else:
            (self._cur_cmd, self._cur_cmd_args, self._cur_callback) = self._cmd_queue.pop(0)

            logger.debug('popped next command: %s'%self._cur_cmd)
            self._send_command()

    @tracer
    def _send_command(self):
        (self._cur_reply_type, self._cur_reply_handler) = self._cmd_map.get(self._cur_cmd, None)

        if not self._cur_reply_type or not self._cur_reply_handler:
            self._execute_callback('Uknown command: %s'%self._cur_cmd, None)

        argstr = ''
        for arg in self._cur_cmd_args:
            argstr += '$%d\r\n%s\r\n'%(len(str(arg)),arg)

        cmdstr = "*%d\r\n$%d\r\n%s\r\n%s" % ( len(self._cur_cmd_args)+1, 
                                                  len(self._cur_cmd),
                                                  self._cur_cmd,
                                                  argstr)

        logger.debug('write: %s'%cmdstr)
        self._stream.write(cmdstr)

        if self._cur_cmd == 'QUIT':
            self._execute_callback(None,'OK')
        elif not self._subscribed:
            logger.debug('read_until(%s)'%self._cur_reply_handler)
            self._stream.read_until('\r\n', self._cur_reply_handler)

    @tracer
    def _handle_status_reply(self, data):
        data = data.strip()
        if not data[0] == '+':
            self._execute_callback('%s'%data[1:], None)
        else:
            self._execute_callback(None, data[1:])

    @tracer
    def _handle_integer_reply(self, data):
        data = data.strip()
        if not data[0] == ':':

            #Check for a nil response
            if data[0] == '$' and data[1:] == '-1':
                self._execute_callback(None, None)
            else:
                self._execute_callback(data[1:], None)
        else:

            #The last thing we get from a subscription is an integer reply
            if self._cur_reply_type == ReplyType.SUBSCRIBE:
                self._cur_multi_bulk_reply_data.append(data[1:])
                self._execute_callback(None, self._cur_multi_bulk_reply_data)
            else:
                self._execute_callback(None, int(data[1:]))

    @tracer
    def _handle_multi_bulk_reply(self, data):
        data = data.strip()
        if not data[0] == '*':
            self._execute_callback('bad multi bulk reply: %s'%data, None)
        else:
            self._cur_multi_bulk_reply_left = int(data[1:])
            logger.debug('self._cur_multi_bulk_reply_left == %d'%self._cur_multi_bulk_reply_left)
            if self._cur_multi_bulk_reply_left <= 0:
                self._execute_callback(None, [None])
            else:
                self._stream.read_until('\r\n', self._handle_bulk_reply)


    @tracer
    def _handle_bulk_reply(self, data):
        data = data.strip()

        #Clear out the current data since this is the start of a new bulk reply
        self._cur_bulk_reply_data = ''

        if not data[0] == '$':
            if self._cur_reply_type == ReplyType.SUBSCRIBE and data[0] == ':':
                self._handle_integer_reply(data)
            else:
                self._execute_callback(data[1:], None)
        else:
            bulk_len = int(data[1:])
            if bulk_len > 0:
                self._stream.read_until('\r\n', partial(self._handle_bulk_reply_data, bulk_len))
            else:
                self._handle_bulk_reply_data(0,None)

    @tracer
    def _handle_bulk_reply_data(self, length, data):
        if data:
            self._cur_bulk_reply_data += data.strip()
            datalen = len(self._cur_bulk_reply_data)

            logger.debug('self._cur_bulk_reply_data=%s.'%self._cur_bulk_reply_data)
        else:
            datalen = 0
            self._cur_bulk_reply_data = None

        logger.debug('datalen: %d, expecting: %d'%(datalen, length))

        if datalen == length:
            if self._cur_bulk_reply_data and not self._cur_bulk_reply_data == '' and self._cur_bulk_reply_data.isdigit():
                self._cur_bulk_reply_data = int(self._cur_bulk_reply_data)

            #This means we are done reading a multi bulk reply
            logger.debug('self._cur_reply_type == %s'%self._cur_reply_type)

            if self._cur_reply_type == ReplyType.MULTI_BULK or self._cur_reply_type == ReplyType.SUBSCRIBE:
                self._cur_multi_bulk_reply_data.append(self._cur_bulk_reply_data)
                self._cur_multi_bulk_reply_left -= 1

                if self._cur_multi_bulk_reply_left == 0:
                    self._execute_callback(None, self._cur_multi_bulk_reply_data)
                else:
                    self._stream.read_until('\r\n', self._handle_bulk_reply)

            elif self._cur_reply_type == ReplyType.BULK:
                self._execute_callback(None, self._cur_bulk_reply_data)
        else:
            logger.debug('we have %d more bytes to read'%(length - datalen))
            self._cur_bulk_reply_data += '\r\n'
            self._stream.read_until('\r\n', partial(self._handle_bulk_reply_data, length))

    @tracer
    def _execute_callback(self, error, value):

        if self._cur_callback:
            self._cur_callback(error, value)

            if not self._cur_reply_type == ReplyType.SUBSCRIBE:
                self._clear_state()
            else:
                self._clear_bulk_data()

        else:
            logger.debug('no current callback')

        #Start all over again 
        if self._subscribed:
            self._stream.read_until('\r\n', self._handle_multi_bulk_reply)
        else:
            self._send_next()

    @tracer
    def _clear_state(self):
        self._cur_cmd = None
        self._cur_cmd_args = None
        self._cur_callback = None
        self._cur_reply_handler = None
        self._cur_reply_type = None
        self._clear_bulk_data()

    @tracer
    def _clear_bulk_data(self):
        self._cur_multi_bulk_reply_left = 0
        self._cur_multi_bulk_reply_data = []
        self._cur_bulk_reply_data = ''

    @tracer
    def _build_cmds(self):
        for cmd in self._cmd_map:
            if cmd == 'DEL':
                name = 'delete'
            else:
                name = cmd.replace(' ','_').lower()

            setattr(self, name, partial(self._queue_command, cmd))

