# Built-in library
import asyncio
import logging

# Third-party library
import rx.operators as ops
from rx.subject import Subject
from rx.scheduler.eventloop import AsyncIOScheduler
from aiohttp import web
from aiopg.sa import create_engine

# Costum library
from handler import OrderHandler

# Log configuration
LOG_REQUEST_FORMAT = logging.Formatter('%(asctime)s|%(level)s|%(message)s')
LOG_REQUEST_HANDLER = logging.FileHandler('request.log')

class Webserver:
    def __init__(self):
        self.app = web.Application()
        self.dbEngine = None
        self.loop = asyncio.get_event_loop()
        self.requestLogger = logging.getLogger('request')
        self.requestLogger.addHandler(LOG_REQUEST_HANDLER)
        self.requestLogger.setLevel(logging.INFO)
        self.loop = asyncio.get_event_loop()
        self.request = Subject() # Requests observable object
        self.subscriptions = [] # List for containing disposable request observer


    def logRequest(self, request : web.Request):
        self.requestLogger.info(f"Incoming request <- {request.remote}|{request.method}|{request.path}")

    # General handler for dispatching request to every request observers
    async def dispatcher(self, request: web.Request) -> web.Response:
        future = asyncio.Future()
        request['future'] = future
        request['loop'] = self.loop
        self.request.on_next(request) # Pass the request to the observers

        await future
        result = future.result() # Get the result response from the observers

        return result

    async def on_shutdown(self, app: web.Application):
        self.dbEngine.close()
        map(lambda i: i.dispose(), self.subscriptions)
        await self.dbEngine.wait_closed()


    async def init(self):
        self.dbEngine = await create_engine(user='reactive', password='reaction', database='reactive', host='localhost')

        # Create disposable request observer for handling order request. Only request to /order will be passed to OrderHandler
        dispose = self.request.pipe(
            ops.do_action(self.logRequest),
            ops.filter(lambda i : i.path == '/order')
        ).subscribe(OrderHandler(self.dbEngine), scheduler=AsyncIOScheduler)
        self.subscriptions.append(dispose)

        self.app.router.add_post('/order', self.dispatcher, name='order')
        self.app.on_shutdown.append(self.on_shutdown)

        return self.app

    def run(self):
        web.run_app(self.init())

webserver = Webserver()
webserver.run()