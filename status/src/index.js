import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import { socket, SocketContext } from './context/socket';
import Website from './website';
  // ========================================
  
  ReactDOM.render(
	<SocketContext.Provider value={socket}>
		{/* <div className="container"> */}
			<Website />
		{/* </div> */}
	</SocketContext.Provider>, document.getElementById('root')
  );
  