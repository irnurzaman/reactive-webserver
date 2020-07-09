import asyncio
from datetime import datetime
from typing import Tuple
from aiohttp import ClientSession
from motor import motor_asyncio, core
from aio_pika import connect_robust, IncomingMessage
import rx.operators as ops
from rx.subject import Subject
from rx.core import Observer
from rx.scheduler.eventloop import AsyncIOScheduler

class OrderHandler(Observer):

    def __init__(self):
        super().__init__()

    def on_next(self, message: Tuple[IncomingMessage, core.AgnosticCollection, dict, asyncio.AbstractEventLoop]):
        loop = message[3]
        async def asyncOrder():
            qmsg = message[0]
            collection = message[1]
            order = message[2]

            await self.matchOrder(order, collection)

            qmsg.ack()

        loop.create_task(asyncOrder())

    async def matchOrder(self, order, collection):

            dictFilter = {'price': {'$lte':order['price']}} if order['action'] == 'BUY' else {'price': {'$gte':order['price']}}
            dictFilter.update({'action': 'BUY' if order['action'] == 'SELL' else 'SELL',
                               'stock': order['stock'],
                               'selected': False,
                               'status': {'$ne': '2'}})
            dictSort = {'price': 1 if order['action'] == 'BUY' else -1,
                        'timestamp': 1}

            orderContra = await collection.find_one_and_update(dictFilter,
                                                               {'$set':{'selected': True}},
                                                               sort=list(dictSort.items()),
                                                               return_document=True)

            while orderContra is not None and order['status'] != '2':
                leaveVol = order['vol'] - order['cumVol']
                contraLeaveVol = orderContra['vol'] - orderContra['cumVol']
                matchVol = contraLeaveVol if contraLeaveVol <= leaveVol else leaveVol
                contraMatchVol = leaveVol if leaveVol <= contraLeaveVol else contraLeaveVol
                order['cumVol'] += matchVol
                orderContra['cumVol'] += contraMatchVol
                order['status'] = '2' if order['cumVol'] == order['vol'] else '1'
                orderContra['status'] = '2' if orderContra['cumVol'] == orderContra['vol'] else '1'

                await collection.find_one_and_update({'orderId': order['orderId']},
                                                     {'$set': {'cumVol': order['cumVol'],
                                                               'status': order['status']}})
                await collection.find_one_and_update({'orderId': orderContra['orderId']},
                                                     {'$set': {'cumVol': orderContra['cumVol'],
                                                               'status': orderContra['status'],
                                                               'selected': False}})

                orderContra = await collection.find_one_and_update(dictFilter,{'$set':{'selected': True}},sort=list(dictSort.items()), return_document=True)

            await collection.find_one_and_update({'orderId': order['orderId']},
                                                 {'$set': {'selected': False}})

class OrderServices:
    def __init__(self):
        self.webservice = ClientSession()
        self.loop = asyncio.get_event_loop()
        self.messages = Subject()
        self.observer = OrderHandler()
        self.disposable = None
        self.orderCollection = None
        self.rmqConn = None
        self.rmqChannel = None

    async def initializer(self):
        self.orderCollection = motor_asyncio.AsyncIOMotorClient('localhost', 27017)
        self.orderCollection = self.orderCollection.reactive.orders

        self.rmqConn = await connect_robust(login='ikhwanrnurzaman', password='123456')
        self.rmqChannel = await self.rmqConn.channel()
        self.orderQueue = await self.rmqChannel.declare_queue('order', durable=True)
        self.disposable = self.messages.subscribe(self.observer, scheduler=AsyncIOScheduler)

        self.loop.create_task(self.rmqListener())

    async def rmqListener(self):
        # Dispatch the consumed message to observer
        await self.orderQueue.consume(self.orderValidation)

    async def orderValidation(self, msg: IncomingMessage):
        data = msg.body.decode()
        now = datetime.now()
        now = now.strftime('%Y-%m-%d %H:%M:%S')
        data = data.split('|')
        account = data[0]
        action = data[1]
        order = data[2].split('.')
        order = {'orderId': order[3],
                 'timestamp': now,
                 'account': account,
                 'action': action,
                 'stock': order[0],
                 'price': int(order[1]),
                 'vol': int(order[2]),
                 'cumVol': 0,
                 'status': '0',
                 'selected': False}

        params = {'account': account if action == 'BUY' else f"{account}.{order['stock']}",
                  'amount': order['price'] * order['vol'] if action == 'BUY' else order['vol']}

        async with self.webservice.request('GET', 'http://localhost:8001/order', json=params) as resp:
            valid = await resp.text()
            if valid == 'OK':
                await self.orderCollection.insert_one(order)
                self.messages.on_next((msg, self.orderCollection, order, self.loop))
            else:
                order['status'] = 'R'
                await self.orderCollection.insert_one(order)
                msg.ack()

    def run(self):
        async def asyncRun():
            await self.initializer()
            await self.rmqListener()

        self.loop.create_task(asyncRun())
        self.loop.run_forever()

if __name__ == '__main__':
    orderService = OrderServices()
    orderService.run()