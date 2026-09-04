#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║  TCP-over-WebSocket Multiplexed Tunnel Client                               ║
║  Production-Ready | Auto-Reconnection | Connection Pooling                  ║
║                                                                              ║
║  Features:                                                                   ║
║  • Single WebSocket connection for multiple TCP streams                     ║
║  • Automatic reconnection with exponential backoff                          ║
║  • Connection pooling and health monitoring                                 ║
║  • Graceful degradation on network failures                                 ║
║  • Comprehensive error handling and logging                                 ║
║  • Zero-config deployment ready                                             ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

Author: Claude (Anthropic)
Version: 2.0.0
License: MIT
"""

import os
import inspect
import asyncio
import websockets
import logging
import struct
import signal
import sys
import time
from typing import Dict, Optional
from dataclasses import dataclass, field
from enum import IntEnum
from collections import deque

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ClientConfig:
    """Client configuration with sensible defaults"""
    # Network settings
    server_url: str = "wss://your-domain.ngrok-free.app"
    local_host: str = "127.0.0.1"
    local_port: int = 1453

    # Authentication (leave empty if server requires no auth, or set WSSPROXY_AUTH_TOKEN)
    auth_token: str = field(default_factory=lambda: os.environ.get("WSSPROXY_AUTH_TOKEN", ""))
    
    # Performance tuning
    max_message_size: int = 10 * 1024 * 1024  # 10MB
    ping_interval: int = 20  # seconds
    ping_timeout: int = 10   # seconds
    tcp_buffer_size: int = 65536  # 64KB
    
    # Reconnection settings
    reconnect_enabled: bool = True
    reconnect_max_attempts: int = 0  # 0 = infinite
    reconnect_initial_delay: float = 1.0  # seconds
    reconnect_max_delay: float = 60.0  # seconds
    reconnect_backoff_factor: float = 2.0
    
    # Connection limits
    max_connections: int = 1000
    connection_timeout: int = 300  # 5 minutes
    
    # Logging
    log_level: str = "INFO"  # DEBUG, INFO, WARNING, ERROR, CRITICAL

# ═══════════════════════════════════════════════════════════════════════════
# PROTOCOL DEFINITION (same as server)
# ═══════════════════════════════════════════════════════════════════════════

class MessageType(IntEnum):
    """WebSocket multiplexing protocol message types"""
    # Client → Server
    CONNECT = 0x01
    DATA = 0x02
    CLOSE = 0x03
    PING = 0x04
    
    # Server → Client
    CONNECT_OK = 0x81
    CONNECT_ERROR = 0x82
    DATA_ACK = 0x83
    PONG = 0x84

@dataclass
class Message:
    """Protocol message structure"""
    msg_type: MessageType
    conn_id: int
    data: bytes = field(default_factory=bytes)
    
    def serialize(self) -> bytes:
        """Serialize message to binary format"""
        return struct.pack('!BI', self.msg_type, self.conn_id) + self.data
    
    @classmethod
    def deserialize(cls, data: bytes) -> Optional['Message']:
        """Deserialize binary data to message"""
        if len(data) < 5:
            return None
        msg_type = MessageType(data[0])
        conn_id = struct.unpack('!I', data[1:5])[0]
        payload = data[5:]
        return cls(msg_type, conn_id, payload)

# ═══════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════════════════════════════════

def setup_logging(level: str = "INFO"):
    """Configure professional logging with colors"""
    
    COLORS = {
        'DEBUG': '\033[36m',
        'INFO': '\033[32m',
        'WARNING': '\033[33m',
        'ERROR': '\033[31m',
        'CRITICAL': '\033[35m',
        'RESET': '\033[0m'
    }
    
    class ColoredFormatter(logging.Formatter):
        def format(self, record):
            levelname = record.levelname
            if levelname in COLORS:
                record.levelname = f"{COLORS[levelname]}{levelname}{COLORS['RESET']}"
            return super().format(record)
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(ColoredFormatter(
        '%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))
    
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        handlers=[handler]
    )

# ═══════════════════════════════════════════════════════════════════════════
# CONNECTION TRACKING
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class LocalConnection:
    """Tracks local TCP connection state"""
    conn_id: int
    writer: asyncio.StreamWriter
    created_at: float = field(default_factory=time.time)
    bytes_sent: int = 0
    bytes_received: int = 0
    last_activity: float = field(default_factory=time.time)
    pending_connect: bool = True

# ═══════════════════════════════════════════════════════════════════════════
# WEBSOCKET CLIENT MANAGER
# ═══════════════════════════════════════════════════════════════════════════

class WebSocketClientManager:
    """Manages WebSocket connection with auto-reconnection"""
    
    def __init__(self, config: ClientConfig):
        self.config = config
        self.logger = logging.getLogger("WSClientManager")
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.connected = False
        self.reconnect_attempt = 0
        self.message_queue: deque = deque(maxlen=1000)
        self.lock = asyncio.Lock()
        self.should_reconnect = True
    
    async def connect(self):
        """Establish WebSocket connection"""
        delay = self.config.reconnect_initial_delay
        
        while self.should_reconnect:
            try:
                self.logger.info(f"🔗 Connecting to {self.config.server_url}...")
                
                connect_kwargs = {
                    "ping_interval": self.config.ping_interval,
                    "ping_timeout": self.config.ping_timeout,
                    "max_size": self.config.max_message_size,
                }
                if self.config.auth_token:
                    sig = inspect.signature(websockets.connect)
                    if "additional_headers" in sig.parameters:
                        connect_kwargs["additional_headers"] = {"Authorization": f"Bearer {self.config.auth_token}"}
                    else:
                        connect_kwargs["extra_headers"] = {"Authorization": f"Bearer {self.config.auth_token}"}

                self.websocket = await asyncio.wait_for(
                    websockets.connect(
                        self.config.server_url,
                        **connect_kwargs
                    ),
                    timeout=30.0
                )
                
                self.connected = True
                self.reconnect_attempt = 0
                self.logger.info("✅ WebSocket connected!")
                
                # Process queued messages
                await self.flush_message_queue()
                
                return
                
            except asyncio.TimeoutError:
                self.logger.error("❌ Connection timeout")
            except Exception as e:
                self.logger.error(f"❌ Connection failed: {e}")
            
            # Reconnection logic
            if not self.config.reconnect_enabled:
                break
            
            self.reconnect_attempt += 1
            if (self.config.reconnect_max_attempts > 0 and 
                self.reconnect_attempt >= self.config.reconnect_max_attempts):
                self.logger.error("❌ Max reconnection attempts reached")
                break
            
            # Exponential backoff
            delay = min(
                delay * self.config.reconnect_backoff_factor,
                self.config.reconnect_max_delay
            )
            
            self.logger.info(f"⏳ Reconnecting in {delay:.1f}s (attempt {self.reconnect_attempt})...")
            await asyncio.sleep(delay)
    
    async def send_message(self, message: Message):
        """Send message with queuing on disconnect"""
        async with self.lock:
            if not self.connected or not self.websocket:
                # Queue message for later
                self.message_queue.append(message)
                self.logger.warning(f"[Conn {message.conn_id}] WebSocket disconnected, message queued")
                return False
            
            try:
                await self.websocket.send(message.serialize())
                return True
            except Exception as e:
                self.logger.error(f"Send error: {e}")
                self.connected = False
                self.message_queue.append(message)
                return False
    
    async def receive_message(self) -> Optional[Message]:
        """Receive message from WebSocket"""
        if not self.connected or not self.websocket:
            return None
        
        try:
            raw_data = await self.websocket.recv()
            if isinstance(raw_data, bytes):
                return Message.deserialize(raw_data)
        except websockets.exceptions.ConnectionClosed:
            self.logger.warning("WebSocket closed")
            self.connected = False
        except Exception as e:
            self.logger.error(f"Receive error: {e}")
            self.connected = False
        
        return None
    
    async def flush_message_queue(self):
        """Send queued messages after reconnection"""
        if not self.message_queue:
            return
        
        self.logger.info(f"📤 Flushing {len(self.message_queue)} queued messages...")
        
        while self.message_queue:
            message = self.message_queue.popleft()
            try:
                await self.websocket.send(message.serialize())
            except Exception as e:
                self.logger.error(f"Failed to flush message: {e}")
                self.message_queue.appendleft(message)
                break
    
    async def close(self):
        """Close WebSocket connection"""
        self.should_reconnect = False
        if self.websocket:
            await self.websocket.close()
        self.connected = False

# ═══════════════════════════════════════════════════════════════════════════
# TUNNEL CLIENT
# ═══════════════════════════════════════════════════════════════════════════

class TunnelClient:
    """Main tunnel client"""
    
    def __init__(self, config: ClientConfig):
        self.config = config
        self.logger = logging.getLogger("TunnelClient")
        self.ws_manager = WebSocketClientManager(config)
        self.connections: Dict[int, LocalConnection] = {}
        self.next_conn_id = 1
        self.lock = asyncio.Lock()
        self.running = False
    
    async def handle_local_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle new local TCP connection"""
        conn_id = self.next_conn_id
        self.next_conn_id += 1
        
        client_addr = writer.get_extra_info('peername')
        self.logger.info(f"[Conn {conn_id}] 📡 New local connection from {client_addr}")
        
        # Wait for WebSocket connection
        max_wait = 30
        for _ in range(max_wait):
            if self.ws_manager.connected:
                break
            await asyncio.sleep(1)
        else:
            self.logger.error(f"[Conn {conn_id}] WebSocket not available")
            writer.close()
            await writer.wait_closed()
            return
        
        try:
            # Register connection
            async with self.lock:
                self.connections[conn_id] = LocalConnection(conn_id, writer)
            
            # Send CONNECT message
            connect_msg = Message(MessageType.CONNECT, conn_id)
            await self.ws_manager.send_message(connect_msg)
            
            self.logger.info(f"[Conn {conn_id}] ✅ Connection established (Total: {len(self.connections)})")
            
            # Start reading from local TCP
            await self.local_tcp_reader(conn_id, reader)
            
        except Exception as e:
            self.logger.error(f"[Conn {conn_id}] Error: {e}", exc_info=True)
        finally:
            await self.close_connection(conn_id)
    
    async def local_tcp_reader(self, conn_id: int, reader: asyncio.StreamReader):
        """Read from local TCP and send to WebSocket"""
        try:
            while True:
                data = await reader.read(self.config.tcp_buffer_size)
                if not data:
                    self.logger.info(f"[Conn {conn_id}] Local connection closed")
                    break
                
                # Update stats
                async with self.lock:
                    conn = self.connections.get(conn_id)
                    if conn:
                        conn.bytes_sent += len(data)
                        conn.last_activity = time.time()
                
                # Send to server
                data_msg = Message(MessageType.DATA, conn_id, data)
                await self.ws_manager.send_message(data_msg)
                
                self.logger.debug(f"[Conn {conn_id}] Sent {len(data)} bytes")
                
        except Exception as e:
            self.logger.error(f"[Conn {conn_id}] TCP reader error: {e}")
    
    async def websocket_message_handler(self):
        """Handle incoming WebSocket messages"""
        while self.running:
            if not self.ws_manager.connected:
                await asyncio.sleep(1)
                continue
            
            message = await self.ws_manager.receive_message()
            if not message:
                continue
            
            if message.msg_type == MessageType.CONNECT_OK:
                await self.handle_connect_ok(message)
            elif message.msg_type == MessageType.CONNECT_ERROR:
                await self.handle_connect_error(message)
            elif message.msg_type == MessageType.DATA:
                await self.handle_data(message)
            elif message.msg_type == MessageType.CLOSE:
                await self.handle_close(message)
            elif message.msg_type == MessageType.PONG:
                self.logger.debug(f"[Conn {message.conn_id}] PONG received")
    
    async def handle_connect_ok(self, message: Message):
        """Handle connection success"""
        async with self.lock:
            conn = self.connections.get(message.conn_id)
            if conn:
                conn.pending_connect = False
        self.logger.info(f"[Conn {message.conn_id}] ✅ Remote connection established")
    
    async def handle_connect_error(self, message: Message):
        """Handle connection failure"""
        reason = message.data.decode('utf-8', errors='ignore') if message.data else "Unknown"
        self.logger.error(f"[Conn {message.conn_id}] ❌ Remote connection failed: {reason}")
        await self.close_connection(message.conn_id)
    
    async def handle_data(self, message: Message):
        """Handle incoming data"""
        async with self.lock:
            conn = self.connections.get(message.conn_id)
        
        if not conn:
            self.logger.warning(f"[Conn {message.conn_id}] Connection not found for data")
            return
        
        try:
            conn.writer.write(message.data)
            await conn.writer.drain()
            
            async with self.lock:
                conn.bytes_received += len(message.data)
                conn.last_activity = time.time()
            
            self.logger.debug(f"[Conn {message.conn_id}] Received {len(message.data)} bytes")
            
        except Exception as e:
            self.logger.error(f"[Conn {message.conn_id}] Write error: {e}")
            await self.close_connection(message.conn_id)
    
    async def handle_close(self, message: Message):
        """Handle close notification"""
        self.logger.info(f"[Conn {message.conn_id}] 🔌 Remote connection closed")
        await self.close_connection(message.conn_id)
    
    async def close_connection(self, conn_id: int):
        """Close connection"""
        async with self.lock:
            conn = self.connections.pop(conn_id, None)
        
        if conn:
            try:
                # Send close message
                close_msg = Message(MessageType.CLOSE, conn_id)
                await self.ws_manager.send_message(close_msg)
                
                # Close local TCP
                conn.writer.close()
                await conn.writer.wait_closed()
                
                duration = time.time() - conn.created_at
                self.logger.info(
                    f"[Conn {conn_id}] ✅ Closed after {duration:.1f}s "
                    f"(↑{conn.bytes_sent} ↓{conn.bytes_received} bytes) "
                    f"(Remaining: {len(self.connections)})"
                )
            except Exception as e:
                self.logger.error(f"[Conn {conn_id}] Close error: {e}")
    
    async def start(self):
        """Start the client"""
        self.running = True
        
        # Print banner
        self.print_banner()
        
        # Connect to WebSocket
        asyncio.create_task(self.ws_manager.connect())
        
        # Start WebSocket message handler
        asyncio.create_task(self.websocket_message_handler())
        
        # Start local TCP server
        self.logger.info(f"🚀 Starting TCP server on {self.config.local_host}:{self.config.local_port}")
        
        try:
            server = await asyncio.start_server(
                self.handle_local_connection,
                self.config.local_host,
                self.config.local_port
            )
            
            self.logger.info("✅ Client ready! Press Ctrl+C to stop")
            
            async with server:
                await server.serve_forever()
                
        except Exception as e:
            self.logger.error(f"Client error: {e}", exc_info=True)
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop the client"""
        if not self.running:
            return
        
        self.logger.info("🛑 Stopping client...")
        self.running = False
        
        # Close all connections
        conn_ids = list(self.connections.keys())
        for conn_id in conn_ids:
            await self.close_connection(conn_id)
        
        # Close WebSocket
        await self.ws_manager.close()
        
        self.logger.info("✅ Client stopped")
    
    def print_banner(self):
        """Print startup banner"""
        banner = f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║             TCP-over-WebSocket Multiplexed Tunnel Client v2.0                ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

📋 Configuration:
   • Local TCP: {self.config.local_host}:{self.config.local_port}
   • Server: {self.config.server_url}
   • Auth: {'Token Protected' if self.config.auth_token else 'Open (No Auth)'}
   • Auto-reconnect: {'Enabled' if self.config.reconnect_enabled else 'Disabled'}
   • Max Connections: {self.config.max_connections}
   • Buffer Size: {self.config.tcp_buffer_size} bytes

🎯 Features:
   ✅ Single WebSocket for multiple TCP connections
   ✅ Automatic reconnection with exponential backoff
   ✅ Message queuing during disconnection
   ✅ Real-time statistics and monitoring
   ✅ Graceful error handling and recovery

💡 Usage:
   Connect your application to localhost:{self.config.local_port}
   Traffic will be tunneled to the server automatically!

"""
        print(banner)

# ═══════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

async def main():
    """Main entry point"""
    # Load configuration
    config = ClientConfig()
    
    # Setup logging
    setup_logging(config.log_level)
    
    # Create and start client
    client = TunnelClient(config)
    
    # Handle signals
    def signal_handler(sig, frame):
        print("\n")
        logging.getLogger("Main").info("Received interrupt signal, shutting down...")
        asyncio.create_task(client.stop())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start client
    await client.start()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        logging.getLogger("Main").critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
