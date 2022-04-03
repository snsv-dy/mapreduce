# Master

import socket as so
from slave import MySocket
from threading import Thread
from functools import partial
from aiohttp import web
import aiohttp_cors
import socketio

async def index(req):
	return web.FileResponse('./gui/test.html')

print("I need your love")
# it's gonn be react and bootstrap react 
static_files = {
	'/': 'gui/test.html'
}

sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
app = web.Application()

cors = 	aiohttp_cors.setup(app)
cors = aiohttp_cors.setup(app, defaults={
    "*": aiohttp_cors.ResourceOptions(
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*",
        )
})
# app.wsgi_app.router.add_get('/', index)
# res = cors.add(app.router.add_resource('/'))
# route = cors.add(res.add_route("GET", index))
# res2 = cors.add(app.router.add_resource('/socket.io'))
# route = cors.add(res2.add_route("GET", index))



app.router.add_get('/', index)
# app.add_routes([web.static('/', './gui')])
# app = socketio.WSGIApp(sio)
# app.router.add_static('/', './status/build')

from TcpServerPart import rundown

@sio.event
async def connect(sid, environ, auth):
	print('connect ', sid)
	current_state = rundown()
	print('current state', current_state)
	await sio.emit('rundown', current_state)
	print('emmited rundown')
	# sio.emit('hey', 'hey')

@sio.event
def disconnect(sid):
    print('disconnect ', sid)

@sio.on('hey')
async def onhey(sid, data):
	print("reveived data", data)
	await sio.emit('hey', {'data': 'hey shinomya'})
	print("emmited")

sio.attach(app)

# from TcpServerPart import sockets
# from threading import Thread

# if __name__ == '__main__':
# 	for resource in app.router._resources:
# 		# Because socket.io already adds cors, if you don't skip socket.io, you get error saying, you've done this already.
# 		print(resource)
# 		if resource.raw_match("/socket.io/"):
# 			continue
# 		cors.add(resource, { '*': aiohttp_cors.ResourceOptions(allow_credentials=True, expose_headers="*", allow_headers="*") })
# 	master_thread = Thread(target=sockets)
# 	master_thread.start()
# 	web.run_app(app)