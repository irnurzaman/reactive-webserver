import asyncio
from typing import Tuple
from aiohttp import web
from aiopg.sa import create_engine, Engine
from aio_pika import connect_robust, IncomingMessage
import rx.operators as ops
from rx.subject import Subject
from rx.core import Observer
from rx.scheduler.eventloop import AsyncIOScheduler

class AccountHandler(Observer):

    def __init__(self):
        super().__init__()
        self.rawSql = {'DEPOSIT': 'UPDATE accounts set balance = balance + %s where account_no = %s',
                       'WITHDRAW': 'UPDATE accounts set balance = balance - %s where account_no = %s'}

    def on_next(self, message: Tuple[IncomingMessage, Engine, dict, asyncio.AbstractEventLoop]):
        loop = message[3]
        async def asyncAccount():
            qmsg = message[0]
            dbEngine = message[1]
            data = message[2]
            action = data['action']
            account = data['account']
            amount = data['amount']

            async with dbEngine.acquire() as dbConn:
                await dbConn.execute(self.rawSql[action], (amount, account))

            qmsg.ack()

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
        self.dbEngine = await create_engine(user='reactive',
                                            password='reaction',
                                            host='localhost',
                                            database='reactive')

        self.rmqConn = await connect_robust(login='ikhwanrnurzaman', password='123456')
        self.rmqChannel = await self.rmqConn.channel()
        self.accountQueue = await self.rmqChannel.declare_queue('account', durable=True)
        self.disposable = self.messages.pipe(ops.map(self.messageProcessor)).subscribe(self.observer, scheduler=AsyncIOScheduler)

        self.app.router.add_get('/order', self.orderValidator, name='order')
        self.loop.create_task(self.rmqListener())

        return self.app

    async def rmqListener(self):
        # Dispatch the consumed message to observer
        await self.accountQueue.consume(self.dispatchMessage)

    def dispatchMessage(self, msg: IncomingMessage):
        self.messages.on_next(msg)

    def messageProcessor(self, message: IncomingMessage) -> Tuple[IncomingMessage, Engine, dict, asyncio.AbstractEventLoop]:
        # Transform message into dictionary and pass object message for acknowledgement, DB engine, and asyncio loop to observer
        data = message.body.decode()
        data = data.split('|')
        data = {'account': data[0],
                'action': data[1],
                'amount': data[2]}

        return (message, self.dbEngine, data, self.loop)

    async def orderValidator(self, request: web.Request) -> web.Response:
        data = await request.json()

        account = data['account']
        amount = data['amount']

        rawSql = 'SELECT balance + balance_hold >= %s validation from accounts where account_no = %s'

        validation = 'BAD'
        async with self.dbEngine.acquire() as dbConn:
            async for row in dbConn.execute(rawSql, (amount, account)):
                row = dict(row)
                validation = 'OK' if row['validation'] else validation

        return web.HTTPOk(text=validation)

    def run(self):
        web.run_app(self.initializer())

if __name__ == '__main__':
    accountService = AccountServices()
    accountService.run()