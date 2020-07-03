# Built-in library
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
        async def asyncOrder(request):
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

        loop.create_task(asyncOrder(request))

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