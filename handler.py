# Built-in library
from uuid import uuid4

# Third-party libray
from rx.core import Observer
from aiohttp import web
from aiopg.sa import Engine


class OrderHandler(Observer):

    def __init__(self, engine: Engine):
        super().__init__()
        self.dbEngine = engine

    def on_next(self, request: web.Request):
        loop = request['loop']
        async def asyncOrder(request):
            future = request['future']

            try:
                params = await request.json()
                clientRef = params.get('client_ref')
                account = params.get('account')
                action = params.get('action')
                stock = params.get('stock')
                price = params.get('price')
                vol = params.get('vol')
                orderId = str(uuid4())[:8]

                rawSql = 'INSERT INTO orders (account, action, stock, price, vol, order_id) VALUES (%s, %s, %s, %s, %s, %s)'
                async with self.dbEngine.acquire() as dbConn:
                    await dbConn.execute(rawSql, (account, action, stock, price, vol, orderId))
                future.set_result(web.json_response({'status':'OK', 'order_id': orderId, 'client_ref':clientRef}))
            except Exception as e:
                future.set_result(web.json_response({'status':'FAIL', 'order_id': None, 'client_ref': None}))

        loop.create_task(asyncOrder(request))