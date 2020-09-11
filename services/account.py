import asyncio
import json
import time
from typing import Tuple
from aiohttp import web
from aiopg.sa import create_engine, Engine
from aio_pika import connect_robust, IncomingMessage
import rx.operators as ops
from rx.subject import Subject
from rx.core import Observer
from rx.scheduler.eventloop import AsyncIOScheduler
import prometheus_client
from prometheus_client import Counter, Summary, Gauge, CONTENT_TYPE_LATEST


# Observer for handling incoming account events
class AccountHandler(Observer):

    def __init__(self):
        super().__init__()
        self.rawSql = {'DEPOSIT': 'UPDATE accounts SET balance = balance + %s WHERE account_no = %s',
                       'WITHDRAW': 'UPDATE accounts SET balance = balance - %s WHERE account_no = %s',
                       'TRADE-BUY': """UPDATE accounts SET balance = balance - %s, balance_hold = balance_hold + %s WHERE account_no = %s;
                                        INSERT INTO accounts VALUES (%s, %s, 0)
                                        ON CONFLICT (account_no)
                                        DO UPDATE SET balance = accounts.balance + %s WHERE accounts.account_no = %s;""",
                       'TRADE-SELL': """UPDATE accounts SET balance = balance - %s, balance_hold = balance_hold + %s WHERE account_no = %s;
                                        UPDATE accounts SET balance = balance + %s WHERE account_no = %s;"""}

        self.params = {'DEPOSIT': lambda i: (i['amount'], i['account']),
                       'WITHDRAW': lambda i: (i['amount'], i['account']),
                       'TRADE-BUY': lambda i: (i['vol']*i['price'], i['vol']*i['price'], i['account'],
                                               f"{i['account']}.{i['stock']}", i['vol'],
                                               i['vol'], f"{i['account']}.{i['stock']}"),
                       'TRADE-SELL': lambda i: (i['vol'], i['vol'], f"{i['account']}.{i['stock']}",
                                               i['vol']*i['price'], i['account'])}

    def on_next(self, message: Tuple[IncomingMessage, Engine, dict, asyncio.AbstractEventLoop]):
        loop = message[3]
        async def asyncAccount():
            qmsg = message[0]
            dbEngine = message[1]
            data = message[2]
            action = data['action']
            params = data['params']
            eventLatency = data['eventLatency']
            eventTime = data['eventTime']
            eventProgress = data['eventProgress']

            async with dbEngine.acquire() as dbConn:
                await dbConn.execute(self.rawSql[action], self.params[action](params))

            qmsg.ack()

            latency = time.time() - eventTime
            eventLatency.labels(data['action']).observe(latency)
            eventProgress.labels(data['action']).dec()

        loop.create_task(asyncAccount())


class AccountServices:
    def __init__(self):
        self.app = web.Application()
        self.loop = asyncio.get_event_loop()
        self.messages = Subject()
        self.observer = AccountHandler()
        self.disposable = None
        self.dbEngine = None
        self.rmqConn = None
        self.rmqChannel = None

    async def initializer(self):
        # Creates  a connection to PostgreSQL database
        self.dbEngine = await create_engine(user='reactive',
                                            password='reaction',
                                            host='localhost',
                                            database='reactive')

        # Creates a connection RabbitMQ
        self.rmqConn = await connect_robust(login='ikhwanrnurzaman', password='123456')
        self.rmqChannel = await self.rmqConn.channel()
        self.accountQueue = await self.rmqChannel.declare_queue('account', durable=True)

        # Subscribes an observer for incoming account events
        self.disposable = self.messages.pipe(ops.map(self.messageProcessor)).subscribe(self.observer, scheduler=AsyncIOScheduler)

        self.app['EVENT_COUNTER'] = Counter('event_counter', 'Total Incoming Event', ('event',), unit='events')
        self.app['EVENT_LATENCY'] = Summary('event_latency', 'Event Process Time', ('event',), unit='seconds')
        self.app['EVENT_PROGRESS'] = Gauge('event_in_progress', 'Event in Progress', ('event',), unit='events')

        # Setup routes for order validation API and account query API
        self.app.router.add_post('/order', self.orderValidator, name='order')
        self.app.router.add_get('/accounts/{account}', self.accountQuery, name='account')
        self.app.router.add_get('/metrics', self.metrics, name='metrics')

        # Start listening to account events from RabbitMQ
        self.loop.create_task(self.rmqListener())

        return self.app

    async def metrics(self, request: web.Request) -> web.Response:
        resp = web.Response(body=prometheus_client.generate_latest())
        resp.content_type = CONTENT_TYPE_LATEST
        return resp

    async def rmqListener(self):
        # Dispatch the consumed message to observer
        await self.accountQueue.consume(self.dispatchMessage)

    def dispatchMessage(self, msg: IncomingMessage):
        self.messages.on_next(msg)

    def messageProcessor(self, message: IncomingMessage) -> Tuple[IncomingMessage, Engine, dict, asyncio.AbstractEventLoop]:
        # Transform message into dictionary and pass object message for acknowledgement, DB engine, the dictionary, and asyncio loop to observer
        data = message.body.decode()
        data = json.loads(data)
        self.app['EVENT_COUNTER'].labels(data['action']).inc()
        self.app['EVENT_PROGRESS'].labels(data['action']).inc()
        data['eventLatency'] = self.app['EVENT_LATENCY']
        data['eventProgress'] = self.app['EVENT_PROGRESS']
        data['eventTime'] = time.time()

        return (message, self.dbEngine, data, self.loop)

    async def orderValidator(self, request: web.Request) -> web.Response:
        startNow = time.time()
        self.app['EVENT_PROGRESS'].labels('ORDER-VALIDATION').inc()
        data = await request.json()

        account = data['account']
        amount = data['amount']

        rawSql = 'UPDATE accounts SET balance_hold = balance_hold - %s WHERE account_no = %s and balance + balance_hold >= %s RETURNING true validation;'

        validation = 'BAD'
        async with self.dbEngine.acquire() as dbConn:
            async for row in dbConn.execute(rawSql, (amount, account, amount)):
                row = dict(row)
                validation = 'OK' if row['validation'] else validation

        latency = time.time() - startNow
        self.app['EVENT_LATENCY'].labels('ORDER-VALIDATION').observe(latency)
        self.app['EVENT_PROGRESS'].labels('ORDER-VALIDATION').dec()
        self.app['EVENT_COUNTER'].labels('ORDER-VALIDATION').inc()

        return web.HTTPOk(text=validation)

    def run(self):
        web.run_app(self.initializer(), port=8001)

    # Account API handler for handling account query
    async def accountQuery(self, request: web.Request) -> web.Response:
        account = request.match_info['account']
        rawSql = "SELECT * FROM accounts where account_no LIKE %s || '%%' ORDER BY account_no;"

        results = []

        async with self.dbEngine.acquire() as dbConn:
            async for row in dbConn.execute(rawSql, account):
                row = dict(row)
                results.append(row)

        return web.json_response({'results': results})


if __name__ == '__main__':
    accountService = AccountServices()
    accountService.run()