import React from 'react';
import io from 'socket.io-client';

const url = 'ws://127.0.0.1:5000'
export const socket = io(url, {transports: ['websocket']});
export const SocketContext = React.createContext();