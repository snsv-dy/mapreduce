import './index.css';
import React, { useContext, useEffect, useState} from 'react';
// import ReactDOM from 'react-dom';
// import SocketIO from "socket.io-client";
import { socket, SocketContext } from './context/socket';

function Test(props) {
	const io = useContext(SocketContext);
	useEffect(() => {
		console.log("effect status: userd");
		io.on('hey', (data) => {
			props.writer('hey ' + JSON.stringify(data));
		});
		
		// io.on('rundown', (data) => {
		// 	console.log('got rundown', data);
		// 	props.writer('Got data ' + JSON.stringify(data));
		// });

		// io.on('rundown', (data) => {props.writer('Got data ' + JSON.stringify(data))});
		// io.on('new worker', (e) => { props.writer('new worker ' + JSON.stringify(e))});
		// io.on('lost worker', (e) => { props.writer('lost worker ' + JSON.stringify(e))});
		io.on('hey', props.hey);
		io.on('rundown', props.rundown);
		io.on('new worker', props.newworker);
		io.on('msg worker', props.msgworker);
		io.on('lost worker', props.lostworker);

		return () => io.disconnect();
	}, []);

	return (
		""
	);
}

class Website extends React.Component {
	constructor(props) {
			super(props);

		var initial_workers = new Map();
		// initial_workers.set()
		// initial_workers.set('192.168.1.14:50120', {status: 'Ready', messages: []});
		// initial_workers.set('192.168.1.14:50134', {status: 'Busy', messages: ['hehe?']});

		this.state = {
			master_address: null,
			workers: initial_workers,
			msgs: ['Not received'],
			selectedWorker: 0
		};
	}

	writeSomething(data) {
		const cur_msg = this.state.msgs;
		this.setState({
				msgs: this.state.msgs.slice().concat([data])
		});
	}

	rundown(data) {
		this.writeSomething('rundown ' + JSON.stringify(data));
		this.setState({master_address: data.master_address})
		for (var w of data.clients) {
			this.newWorker(w);
		}
	}

	newWorker(data) {
		var workers = new Map(this.state.workers);
		if (workers.has(data.address)) {
			console.log("WARNING: New worker already in list (duplicate ip address)");
		}

		workers.set(data.address, {status: data.status, messages: []});
		this.setState({workers: workers});
	}

	lostWorker(data) {
		var workers = new Map(this.state.workers);
		if (!workers.has(data.address)) {
			console.log("WARNING: Attempting to remove nonexistent client. (no ip address in map)");
		}

		workers.delete(data.address);
		var newstate = {workers: workers};
		if (this.state.selectedWorker == data.address) {
			newstate.selectedWorker = '';
		}
		this.setState(newstate);
	}

	hey(data) {
		this.writeSomething('hey ' + JSON.stringify(data));
	}

	msgFromWorker(data) {
		if (this.state.workers.has(data.address)) {
			var wks = new Map(this.state.workers);
			var worker = wks.get(data.address);
			worker.messages.push(data.message);

			this.setState({
				workers: wks
			})
		}
	}

	workerClicked(i_e) {
		this.setState({selectedWorker: i_e}); 
		console.log('selectedWorker ' + this.state.selectedWorker + ", i: " + i_e);
	}

	render() {
		const workers = [];
		
		// let or bind handler, if parameter passed to handler is same for all elements.
		// onClick={this.workerClicked.bind(this, i)}
		const worker_elements = this.state.workers.entries();
		for (let i = 0; i < this.state.workers.size; i++) {
			const [ip, w] = worker_elements.next().value;
			workers.push(
				<li key={ip}
				onClick={(e) => {this.workerClicked(ip)}} 
				className={this.state.selectedWorker === ip ? 'active' : ''}>
					<p>Address: {ip} [{i}]</p>
					<p>Status : {w.status}</p>
					<hr />
				</li>
			);
		}

		const messages = [];
		for (let m of this.state.msgs) {
			messages.push(<p>{m}</p>);
		}

		var right = null;

		if (this.state.selectedWorker != '') {
			const ip = this.state.selectedWorker;
			const worker = this.state.workers.get(ip);
			// const mes = this.state.workers.get(this.state.selectedWorker).messages.map(function(o, i) {
			// 	return <li>{o} + asdsad</li>;
			// });

			// console.log('mes', mes);	
			right = (
				<div className="right">
					<button onClick={(e) => this.setState({selectedWorker: ''})}>Close</button>
					<h1>Worker {ip}</h1>
					<p>Status: Redy</p>
					<p>Jobname: ...</p>
					<p>Messages idk:</p>
					<ul>
						{worker.messages.map(function(o, i) {
								return <li>{o}</li>
							 }
						)}
					</ul>
				</div>
			);
		}

		return (
		<div className="website">
			<div className="left">
				<div className="masterInfo">
					<h1>Master of mapreduce</h1>
					<h3>Listening on {this.state.master_address}</h3>
				</div>
				<p>Connected workers:</p>
				<hr />
				<div className="workers">
					<ul>
						{workers}
					</ul>
				</div>
				<div>
					{messages}
				</div>
			</div>
			{right}
			<Test 
				writer={(e) => this.writeSomething(e)}
				hey={(e) => this.writeSomething(e)} 
				rundown={(e) => this.rundown(e)}
				newworker={(e) => this.newWorker(e)}
				lostworker={(e) => this.lostWorker(e)}
				msgworker={(e) => this.msgFromWorker(e)}
				
			/>
		</div>
		)
	}
}

export default Website;