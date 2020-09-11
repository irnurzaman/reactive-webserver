import asyncio
import json
from datetime import datetime
from typing import Tuple
from aiohttp import web, ClientSession
from motor import motor_asyncio, core
from aio_pika import connect_robust, IncomingMessage, Message, DeliveryMode, RobustChannel
from rx.subject import Subject
from rx.core import Observer
from rx.scheduler.eventloop import AsyncIOScheduler


# Observer for matching any incoming order
class OrderMatcher(Observer):

    def __init__(self, loop: asyncio.AbstractEventLoop, collection: core.AgnosticCollection, channel: RobustChannel):
        super().__init__()
        self.loop = loop
        self.collection = collection
        self.channel = channel
        self.tradeNotif = Subject()
        self.tradeNotifier = self.tradeNotif.subscribe(TradeNotifier(self.loop, self.channel))

    def on_next(self, message: Tuple[IncomingMessage, dict]):
        async def asyncOrder():
            qmsg = message[0]
            order = message[1]

            await self.matchOrder(order)

            qmsg.ack()

        self.loop.create_task(asyncOrder())

    # Match incoming order to existing orders in MongoDB and send any matched order to trade observable
    async def matchOrder(self, order):

            dictFilter = {'price': {'$lte':order['price']}} if order['action'] == 'BUY' else {'price': {'$gte':order['price']}}
            dictFilter.update({'action': 'BUY' if order['action'] == 'SELL' else 'SELL',
                               'stock': order['stock'],
                               'selected': False,
                               'status': {'$nin': ['2','R']}})
            dictSort = {'price': 1 if order['action'] == 'BUY' else -1,
                        'timestamp': 1}

            orderContra = await self.collection.find_one_and_update(dictFilter,
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

                await self.collection.find_one_and_update({'orderId': order['orderId']},
                                                     {'$set': {'cumVol': order['cumVol'],
                                                               'status': order['status']}})
                tradeInit = {'action': order['action'], 'account': order['account'], 'stock':order['stock'],
                             'price':order['price'], 'vol': matchVol}
                self.tradeNotif.on_next(tradeInit)

                await self.collection.find_one_and_update({'orderId': orderContra['orderId']},
                                                     {'$set': {'cumVol': orderContra['cumVol'],
                                                               'status': orderContra['status'],
                                                               'selected': False}})
                tradeContra = {'action': orderContra['action'], 'account': orderContra['account'], 'stock': orderContra['stock'],
                             'price': orderContra['price'], 'vol': contraMatchVol}
                self.tradeNotif.on_next(tradeContra)

                if order['status'] != '2':
                    orderContra = await self.collection.find_one_and_update(dictFilter,{'$set':{'selected': True}},sort=list(dictSort.items()), return_document=True)

            await self.collection.find_one_and_update({'orderId': order['orderId']},
                                                 {'$set': {'selected': False}})

# Observer for notifying any trade event to account services
class TradeNotifier(Observer):
    def __init__(self, loop: asyncio.AbstractEventLoop, channel: RobustChannel):
        super().__init__()
        self.loop = loop
        self.channel = channel

    def on_next(self, data: dict):

        # Send trade event to RabbitMQ
        async def asyncTrade():
            msg = json.dumps({'action': f"TRADE-{data['action']}", 'params': data})
            msg = Message(msg.encode(), delivery_mode=DeliveryMode.PERSISTENT)
            await self.channel.default_exchange.publish(msg, routing_key='account')

        self.loop.create_task(asyncTrade())

class OrderServices:
    def __init__(self):
        self.app = web.Application()
        self.webservice = ClientSession()
        self.loop = asyncio.get_event_loop()
        self.messages = Subject()
        self.disposable = None
        self.orderCollection = None
        self.rmqConn = None
        self.rmqChannel = None

    async def initializer(self):
        # Creates a connection to MongoDB
        self.orderCollection = motor_asyncio.AsyncIOMotorClient('localhost', 27017)
        self.orderCollection = self.orderCollection.reactive.orders

        # Creates a connection to RabbitMQ
        self.rmqConn = await connect_robust(login='ikhwanrnurzaman', password='123456')
        self.rmqChannel = await self.rmqConn.channel()
        self.orderQueue = await self.rmqChannel.declare_queue('order', durable=True)

        # Subscribes an observer for incoming order events
        self.disposable = self.messages.subscribe(OrderMatcher(self.loop, self.orderCollection, self.rmqChannel), scheduler=AsyncIOScheduler)

        # Start listening to order events from RabbitMQ
        self.loop.create_task(self.rmqListener())

        # Setup router for order API
        self.app.router.add_get('/orders/{account}', self.orderQuery, name='orders')

        return self.app

    # order API handler for handling order query
    async def orderQuery(self, request: web.Request) -> web.Response:
        account = request.match_info['account']
        orders = []
        async for doc in self.orderCollection.find({'account': account}).sort('timestamp', -1):
            doc.pop('_id', None)
            doc.pop('selected', None)
            orders.append(doc)

        return web.json_response({'results': orders})

    async def rmqListener(self):
        # Dispatch the consumed message to observer
        await self.orderQueue.consume(self.orderValidation)

    # Validate orders by checking account availability
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

        # Check account availability to account services API
        async with self.webservice.request('POST', 'http://localhost:8001/order', json=params) as resp:
            valid = await resp.text()
            if valid == 'OK':
                await self.orderCollection.insert_one(order)
                self.messages.on_next((msg, order)) # Send valid order to order observable
            else:
                order['status'] = 'R'
                await self.orderCollection.insert_one(order)
                msg.ack()

    def run(self):
        web.run_app(self.initializer(), port=8002)

if __name__ == '__main__':
    orderService = OrderServices()
    orderService.run()