# Built-in library
import json
from uuid import uuid4

# Third-party libray
from rx.core import Observer
from aiohttp import web
from aio_pika import Message, DeliveryMode


class OrderHandler(Observer):

    def __init__(self):
        super().__init__()

    def on_next(self, request: web.Request):
        loop = request['loop']
        async def asyncOrder():
            future = request['future']
            body = request['body']
            channel = request['channel']
            queue = request['queue']

            clientRef = body['client_ref']
            account = body['account']
            action = body['action']
            stock = body['stock']
            price = body['price']
            vol = body['vol']
            orderId = str(uuid4())[:8]

            msg = f"{account}|{action}|{stock}.{price}.{vol}.{orderId}"

            msg = Message(msg.encode(), delivery_mode=DeliveryMode.PERSISTENT)

            await channel.default_exchange.publish(msg, routing_key=queue)

            future.set_result(web.json_response({'status':'OK', 'order_id': orderId, 'client_ref':clientRef}))

        loop.create_task(asyncOrder())

    def orderVerificator(self, request: web.Request) -> bool:
        body = request['body']
        future = request['future']
        verified = True

        paramsExpect = ('client_ref', 'account', 'action', 'stock', 'price', 'vol')

        paramsMissing = list(filter(lambda i: i not in body, paramsExpect))

        if len(paramsMissing) > 0:
            body['verified'] = False
            future.set_result(web.json_response({'status':'FAIL', 'order_id': None, 'client_ref': None, 'reason':f"Missing parameter {','.join(paramsMissing)}"},
                                                status=422))
            return False

        if body['action'] not in ('BUY', 'SELL'):
            verified = False
            reason = 'Action must be BUY or SELL'

        if not isinstance(body['price'], int) or not isinstance(body['vol'], int):
            verified = False
            reason = 'Price and volume must be integer'

        elif body['price'] <= 0 or body['vol'] <= 0:
            verified = False
            reason = 'Price and volume must be > 0'

        if not verified:
            future.set_result(web.json_response({'status':'FAIL', 'order_id': None, 'client_ref': body['client_ref'], 'reason': reason},
                                                status=422))

        return verified

class AccountHandler(Observer):

    def __init__(self):
        super().__init__()

    def on_next(self, request: web.Request):
        loop = request['loop']
        async def asyncAccount():
            body = request['body']
            future = request['future']
            channel = request['channel']
            queue = request['queue']

            clientRef = body['client_ref']
            action = body['action']
            account = body['params']['account']
            amount = body['params']['amount']
            requestId = str(uuid4())[:8]

            msg = json.dumps({'action':action, 'params':{'account':account, 'amount':amount}})

            msg = Message(msg.encode(), delivery_mode=DeliveryMode.PERSISTENT)

            await channel.default_exchange.publish(msg, routing_key=queue)

            future.set_result(web.json_response({'status':'OK', 'request_id': requestId, 'client_ref':clientRef}))

        loop.create_task(asyncAccount())

    def accountVerificator(self, request: web.Request) -> bool:
        body = request['body']
        future = request['future']
        verified = True

        paramsExpect = ('client_ref', 'action', 'params')

        paramsMissing = list(filter(lambda i: i not in body, paramsExpect))

        if len(paramsMissing) > 0:
            future.set_result(web.json_response({'status':'FAIL', 'request_id': None, 'client_ref': None,
                                                 'reason':f"Missing parameter {', '.join(paramsMissing)}"},
                                                status=422))
            return False

        if not isinstance(body['params'], dict):
            future.set_result(web.json_response({'status': 'FAIL', 'request_id': None, 'client_ref': None,
                                                 'reason': f"params field is not an object"}, status=422))
            return False

        if body['action'] not in ('DEPOSIT', 'WITHDRAW'):
            verified = False
            reason = 'Action must be DEPOSIT or WITHDRAW'

        if not isinstance(body['params'].get('amount'), int):
            verified = False
            reason = 'Amount must be integer'

        elif body['params']['amount'] <= 0 :
            verified = False
            reason = 'Amount must be > 0'

        if body['params'].get('account') is None:
            verified = False
            reason = 'Missing account parameter'

        if not verified:
            future.set_result(web.json_response({'status':'FAIL', 'request_id': None, 'client_ref': body['client_ref'], 'reason': reason},
                                                status=422))

        return verified