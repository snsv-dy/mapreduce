from flask import Flask, send_from_directory, send_file
import socketio

sio = socketio.Server(async_mode='threading', cors_allowed_origins='*')
app = Flask(__name__, static_url_path='/gui/')
app.wsgi_app = socketio.WSGIApp(sio, app.wsgi_app)

# ... Socket.IO and Flask handler functions ...

from TcpServerPart import rundown, execute_job

@sio.event
def connect(sid, environ, auth):
	print("connected", sid)
	sio.emit('rundown', rundown())

@sio.event
def disconnect(sid):
	print("disconnected")

@sio.event
def jobexec(sid, data):
	print("got jobexec data ", data)
	execute_job()

@app.route('/')
def index():
	return send_file('gui/test.html')

# from threading import Thread
# from TcpServerPart import sockets

# if __name__ == '__main__':
# 	master_thread = Thread(target=sockets)
# 	master_thread.start()
# 	app.run()
