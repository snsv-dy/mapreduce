from flask import Flask, send_from_directory, send_file, request
import socketio

sio = socketio.Server(async_mode='threading', cors_allowed_origins='*')
app = Flask(__name__, static_url_path='/gui/')
app.wsgi_app = socketio.WSGIApp(sio, app.wsgi_app)

# ... Socket.IO and Flask handler functions ...

from TcpServerPart import rundown, execute_job, check_job_status

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
	execute_job('map_functor')

@app.route('/')
def index():
	return send_file('gui/test.html')

@app.route('/start/<string:file>/<string:data>')
def start(file, data):
	if file == None or data == None:
		return {'error': 'Please provide implementation and data files.'}

	job_resp = execute_job(file, data)

	if 'error' not in job_resp:
		job_resp['check_status'] = request.url.replace('start', 'status').replace(file, job_resp['id']).replace('/' + data, '')

	return job_resp

@app.route('/status/<string:id>')
def status(id):
	return check_job_status(id)

# from threading import Thread
# from TcpServerPart import sockets

# if __name__ == '__main__':
# 	master_thread = Thread(target=sockets)
# 	master_thread.start()
# 	app.run()
