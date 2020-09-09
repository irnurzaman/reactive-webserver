# Built-in library
import asyncio
import logging
import time

# Third-party library
import rx.operators as ops
import prometheus_client
from prometheus_client import Gauge, Histogram, Counter, CONTENT_TYPE_LATEST
from rx.subject import Subject
from rx.scheduler.eventloop import AsyncIOScheduler
from aiohttp import web, ClientSession
from aio_pika import connect_robust

# Costum library
from handler import OrderHandler, AccountHandler

# Log configuration
LOG_REQUEST_FORMAT = logging.Formatter('%(asctime)s|%(levelname)s|%(message)s')
LOG_REQUEST_HANDLER = logging.FileHandler('request.log')
LOG_REQUEST_HANDLER.setFormatter(LOG_REQUEST_FORMAT)

class Webserver:
    def __init__(self):
        self.app = web.Application()
        self.webservice = ClientSession()
        self.loop = asyncio.get_event_loop()
        self.request = Subject() # Requests observable object
        self.subscriptions = [] # List for containing disposable request observer
        self.rmqConn = None
        self.channel = {}
        self.orderHandler = OrderHandler()
        self.accountHandler = AccountHandler()
        self.requestLogger = logging.getLogger('request')
        self.requestLogger.addHandler(LOG_REQUEST_HANDLER)
        self.requestLogger.setLevel(logging.INFO)


    def logRequest(self, request : web.Request):
        self.requestLogger.info(f"Incoming request <- {request.remote}|{request.method}|{request.path}")

    # General handler for dispatching request to every request observers
    async def dispatcher(self, request: web.Request) -> web.Response:
        future = asyncio.Future()
        request['future'] = future
        request['loop'] = self.loop

        try:
            body = await request.json()
            request['body'] = body
            request['queue'] = request.path.replace('/','')
            request['channel'] = self.channel[request.path]
        except Exception as e:
            return web.json_response({'status':'FAIL', 'order_id': None, 'client_ref': None, 'reason':'Failed to parse request body'}, status=400)

        self.request.on_next(request) # Pass the request to the observers

        await future
        result = future.result() # Get the result response from the observers

        return result

    async def accountQuery(self, request: web.Request) -> web.Response:
        startTime = time.time()
        self.app['REQUEST_PROGRESS'].labels(request.path, request.method).inc()
        account = request.match_info['account']

        response = {'results': []}
        async with self.webservice.get(f'http://localhost:8001/accounts/{account}') as resp:
            response = await resp.json()

        latency = time.time() - startTime
        self.app['REQUEST_LATENCY'].labels(request.path, request.method).observe(latency)
        self.app['REQUEST_PROGRESS'].labels(request.path, request.method).dec()
        self.app['REQUEST_COUNT'].labels(request.path, request.method).inc()

        return web.json_response(response)

    async def orderQuery(self, request: web.Request) -> web.Response:
        startTime = time.time()
        self.app['REQUEST_PROGRESS'].labels(request.path, request.method).inc()
        account = request.match_info['account']

        response = {'results': []}
        async with self.webservice.get(f'http://localhost:8002/orders/{account}') as resp:
            response = await resp.json()

        latency = time.time() - startTime
        self.app['REQUEST_LATENCY'].labels(request.path, request.method).observe(latency)
        self.app['REQUEST_PROGRESS'].labels(request.path, request.method).dec()
        self.app['REQUEST_COUNT'].labels(request.path, request.method).inc()

        return web.json_response(response)

    async def metrics(self, request: web.Request) -> web.Response:
        resp = web.Response(body=prometheus_client.generate_latest())
        resp.content_type = CONTENT_TYPE_LATEST
        return resp

    async def on_shutdown(self, app: web.Application):
        await self.rmqConn.close()
        map(lambda i: i.dispose(), self.subscriptions)


    async def init(self):
        self.app['REQUEST_COUNT'] = Counter('request_total', 'Total Incoming Request', ('path', 'method'), unit='requests')
        self.app['REQUEST_LATENCY'] = Histogram('request_latency', 'Request Process Time', ('path', 'method'), unit='seconds')
        self.app['REQUEST_PROGRESS'] = Gauge('request_progress', 'Request in Progress', ('path', 'method'), unit='requests')

        # Establish connection to RabbitMQ
        self.rmqConn = await connect_robust(login='ikhwanrnurzaman', password='123456')

        # Establish channel for order request and declare order queue
        self.channel['/order'] = await self.rmqConn.channel()
        await self.channel['/order'].declare_queue('order', durable=True)

        # Establish channel for account request and declare account queue
        self.channel['/account'] = await self.rmqConn.channel()
        await self.channel['/account'].declare_queue('account', durable=True)

        # Create disposable request observer for handling order request. Only request to /order will be passed to OrderHandler
        dispose = self.request.pipe(
            ops.filter(lambda i : i.path == '/order'),
            ops.do_action(self.logRequest),
            ops.filter(self.orderHandler.orderVerificator)
        ).subscribe(self.orderHandler, scheduler=AsyncIOScheduler)
        self.subscriptions.append(dispose)

        # Create disposable request observer for handling account request. Only request to /account will be passed to OrderHandler
        dispose = self.request.pipe(
            ops.filter(lambda i : i.path == '/account'),
            ops.do_action(self.logRequest),
            ops.filter(self.accountHandler.accountVerificator)
        ).subscribe(self.accountHandler, scheduler=AsyncIOScheduler)
        self.subscriptions.append(dispose)

        self.app.router.add_post('/order', self.dispatcher, name='order')
        self.app.router.add_get('/orders/{account}', self.orderQuery)
        self.app.router.add_post('/account', self.dispatcher, name='account')
        self.app.router.add_get('/accounts/{account}', self.accountQuery)
        self.app.router.add_get('/metrics', self.metrics)
        self.app.on_shutdown.append(self.on_shutdown)

        return self.app

    def run(self):
        web.run_app(self.init())

webserver = Webserver()
webserver.run()