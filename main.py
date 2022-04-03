from eas import app
from TcpServerPart import sockets
from threading import Thread
import socketio

if __name__ == '__main__':
	master_thread = Thread(target=sockets)
	master_thread.start()
	app.run()