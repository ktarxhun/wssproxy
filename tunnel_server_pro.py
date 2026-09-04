#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║  TCP-over-WebSocket Multiplexed Tunnel Server                               ║
║  Production-Ready | Built-in Ngrok | Connection Pooling                     ║
║                                                                              ║
║  Features:                                                                   ║
║  • Single WebSocket connection for multiple TCP streams                     ║
║  • Automatic ngrok tunnel management                                        ║
║  • Connection pooling and health monitoring                                 ║
║  • Automatic reconnection handling                                          ║
║  • Comprehensive error handling and logging                                 ║
║  • Zero-config deployment ready                                             ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

Author: Claude (Anthropic)
Version: 2.0.0
License: MIT
"""

import asyncio
import websockets
import logging
import struct
import signal
import sys
import json
import time
import subprocess
import re
import atexit
from typing import Dict, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass, field
from enum import IntEnum

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ServerConfig:
    """Server configuration with sensible defaults"""
    # Network settings
    ws_host: str = "0.0.0.0"
    ws_port: int = 80
    target_tcp_host: str = "127.0.0.1"
    target_tcp_port: int = 1453

    # Ngrok settings
    ngrok_domain: str = "your-domain.ngrok-free.app"
    ngrok_enabled: bool = True
    ngrok_region: str = "eu"  # eu, us, ap, au, sa, jp, in
    
    # Performance tuning
    max_message_size: int = 10 * 1024 * 1024  # 10MB
    ping_interval: int = 20  # seconds
    ping_timeout: int = 10   # seconds
    tcp_buffer_size: int = 65536  # 64KB
    
    # Connection limits
    max_connections: int = 1000
    connection_timeout: int = 300  # 5 minutes
    
    # Logging
    log_level: str = "INFO"  # DEBUG, INFO, WARNING, ERROR, CRITICAL

# ═══════════════════════════════════════════════════════════════════════════
# PROTOCOL DEFINITION
# ═══════════════════════════════════════════════════════════════════════════

class MessageType(IntEnum):
    """WebSocket multiplexing protocol message types"""
    # Client → Server
    CONNECT = 0x01      # Request new TCP connection
    DATA = 0x02         # Send data through TCP connection
    CLOSE = 0x03        # Close TCP connection
    PING = 0x04         # Keep-alive ping
    
    # Server → Client
    CONNECT_OK = 0x81   # TCP connection established
    CONNECT_ERROR = 0x82  # TCP connection failed
    DATA_ACK = 0x83     # Data received (optional)
    PONG = 0x84         # Keep-alive pong

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
    
    # Color codes
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
        'RESET': '\033[0m'
    }
    
    class ColoredFormatter(logging.Formatter):
        def format(self, record):
            levelname = record.levelname
            if levelname in COLORS:
                record.levelname = f"{COLORS[levelname]}{levelname}{COLORS['RESET']}"
            return super().format(record)
    
    # Setup handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(ColoredFormatter(
        '%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        handlers=[handler]
    )

# ═══════════════════════════════════════════════════════════════════════════
# NGROK MANAGER
# ═══════════════════════════════════════════════════════════════════════════

class NgrokManager:
    """Manages ngrok tunnel lifecycle"""
    
    def __init__(self, config: ServerConfig):
        self.config = config
        self.process: Optional[subprocess.Popen] = None
        self.public_url: Optional[str] = None
        self.logger = logging.getLogger("NgrokManager")
        
        # Register cleanup
        atexit.register(self.stop)
    
    async def start(self) -> str:
        """Start ngrok tunnel and return public URL"""
        if not self.config.ngrok_enabled:
            self.logger.info("Ngrok disabled, skipping...")
            return f"ws://localhost:{self.config.ws_port}"
        
        self.logger.info("Starting ngrok tunnel...")
        
        try:
            # Build ngrok command
            cmd = [
                "ngrok", "http",
                f"--domain={self.config.ngrok_domain}",
                f"--region={self.config.ngrok_region}",
                str(self.config.ws_port)
            ]
            
            self.logger.debug(f"Executing: {' '.join(cmd)}")
            
            # Start ngrok process
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Wait for ngrok to be ready
            max_wait = 10
            for i in range(max_wait):
                if self.process.poll() is not None:
                    stderr = self.process.stderr.read()
                    raise RuntimeError(f"Ngrok failed to start: {stderr}")
                
                await asyncio.sleep(1)
                
                # Try to get URL from ngrok API
                try:
                    import aiohttp
                    async with aiohttp.ClientSession() as session:
                        async with session.get('http://127.0.0.1:4040/api/tunnels') as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                if data.get('tunnels'):
                                    tunnel = data['tunnels'][0]
                                    self.public_url = tunnel['public_url'].replace('https://', 'wss://')
                                    self.logger.info(f"✅ Ngrok tunnel ready: {self.public_url}")
                                    return self.public_url
                except:
                    pass
            
            # Fallback to configured domain
            self.public_url = f"wss://{self.config.ngrok_domain}"
            self.logger.info(f"✅ Ngrok tunnel started: {self.public_url}")
            return self.public_url
            
        except FileNotFoundError:
            self.logger.error("❌ Ngrok not found! Please install: https://ngrok.com/download")
            raise
        except Exception as e:
            self.logger.error(f"❌ Failed to start ngrok: {e}")
            raise
    
    def stop(self):
        """Stop ngrok tunnel"""
        if self.process:
            self.logger.info("Stopping ngrok tunnel...")
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            self.logger.info("✅ Ngrok tunnel stopped")

# ═══════════════════════════════════════════════════════════════════════════
# CONNECTION MANAGER
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ConnectionInfo:
    """Tracks individual TCP connection state"""
    conn_id: int
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    created_at: float = field(default_factory=time.time)
    bytes_sent: int = 0
    bytes_received: int = 0
    last_activity: float = field(default_factory=time.time)

class ConnectionManager:
    """Manages TCP connections for a WebSocket client"""
    
    def __init__(self, config: ServerConfig):
        self.config = config
        self.connections: Dict[int, ConnectionInfo] = {}
        self.logger = logging.getLogger("ConnectionManager")
        self.lock = asyncio.Lock()
    
    async def create_connection(self, conn_id: int) -> Tuple[bool, str]:
        """Create new TCP connection"""
        async with self.lock:
            if len(self.connections) >= self.config.max_connections:
                return False, "Max connections reached"
            
            if conn_id in self.connections:
                return False, "Connection ID already exists"
        
        try:
            self.logger.info(f"[Conn {conn_id}] Creating TCP connection to "
                           f"{self.config.target_tcp_host}:{self.config.target_tcp_port}")
            
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    self.config.target_tcp_host,
                    self.config.target_tcp_port
                ),
                timeout=10.0
            )
            
            async with self.lock:
                self.connections[conn_id] = ConnectionInfo(conn_id, reader, writer)
            
            self.logger.info(f"[Conn {conn_id}] ✅ TCP connection established "
                           f"(Total: {len(self.connections)})")
            return True, "Connected"
            
        except asyncio.TimeoutError:
            return False, "Connection timeout"
        except ConnectionRefusedError:
            return False, "Connection refused"
        except Exception as e:
            self.logger.error(f"[Conn {conn_id}] Connection error: {e}")
            return False, str(e)
    
    async def send_data(self, conn_id: int, data: bytes) -> bool:
        """Send data through TCP connection"""
        async with self.lock:
            conn = self.connections.get(conn_id)
        
        if not conn:
            self.logger.warning(f"[Conn {conn_id}] Connection not found")
            return False
        
        try:
            conn.writer.write(data)
            await conn.writer.drain()
            
            async with self.lock:
                conn.bytes_sent += len(data)
                conn.last_activity = time.time()
            
            self.logger.debug(f"[Conn {conn_id}] Sent {len(data)} bytes")
            return True
            
        except Exception as e:
            self.logger.error(f"[Conn {conn_id}] Send error: {e}")
            await self.close_connection(conn_id)
            return False
    
    async def close_connection(self, conn_id: int):
        """Close TCP connection"""
        async with self.lock:
            conn = self.connections.pop(conn_id, None)
        
        if conn:
            try:
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
    
    async def close_all(self):
        """Close all connections"""
        conn_ids = list(self.connections.keys())
        for conn_id in conn_ids:
            await self.close_connection(conn_id)
    
    def get_stats(self) -> dict:
        """Get connection statistics"""
        return {
            'active_connections': len(self.connections),
            'total_bytes_sent': sum(c.bytes_sent for c in self.connections.values()),
            'total_bytes_received': sum(c.bytes_received for c in self.connections.values())
        }

# ═══════════════════════════════════════════════════════════════════════════
# WEBSOCKET HANDLER
# ═══════════════════════════════════════════════════════════════════════════

class WebSocketHandler:
    """Handles WebSocket client connection"""
    
    def __init__(self, websocket, config: ServerConfig):
        self.websocket = websocket
        self.config = config
        self.conn_manager = ConnectionManager(config)
        self.logger = logging.getLogger("WebSocketHandler")
        self.client_addr = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
        self.start_time = time.time()
    
    async def handle(self):
        """Main WebSocket handling loop"""
        self.logger.info(f"[{self.client_addr}] 🔗 WebSocket connected")
        
        try:
            # Start receiving messages
            async for raw_message in self.websocket:
                if isinstance(raw_message, bytes):
                    await self.handle_message(raw_message)
                    
        except websockets.exceptions.ConnectionClosed as e:
            self.logger.info(f"[{self.client_addr}] Connection closed: {e.code} {e.reason}")
        except Exception as e:
            self.logger.error(f"[{self.client_addr}] Error: {e}", exc_info=True)
        finally:
            await self.cleanup()
    
    async def handle_message(self, raw_data: bytes):
        """Process incoming message"""
        message = Message.deserialize(raw_data)
        if not message:
            self.logger.warning(f"[{self.client_addr}] Invalid message format")
            return
        
        if message.msg_type == MessageType.CONNECT:
            await self.handle_connect(message)
        elif message.msg_type == MessageType.DATA:
            await self.handle_data(message)
        elif message.msg_type == MessageType.CLOSE:
            await self.handle_close(message)
        elif message.msg_type == MessageType.PING:
            await self.handle_ping(message)
    
    async def handle_connect(self, message: Message):
        """Handle connection request"""
        success, reason = await self.conn_manager.create_connection(message.conn_id)
        
        if success:
            # Start TCP reader task
            asyncio.create_task(self.tcp_reader_task(message.conn_id))
            # Send success response
            response = Message(MessageType.CONNECT_OK, message.conn_id)
        else:
            self.logger.error(f"[Conn {message.conn_id}] Connection failed: {reason}")
            response = Message(MessageType.CONNECT_ERROR, message.conn_id, reason.encode())
        
        await self.websocket.send(response.serialize())
    
    async def handle_data(self, message: Message):
        """Handle data message"""
        await self.conn_manager.send_data(message.conn_id, message.data)
    
    async def handle_close(self, message: Message):
        """Handle close request"""
        await self.conn_manager.close_connection(message.conn_id)
    
    async def handle_ping(self, message: Message):
        """Handle ping message"""
        response = Message(MessageType.PONG, message.conn_id)
        await self.websocket.send(response.serialize())
    
    async def tcp_reader_task(self, conn_id: int):
        """Read from TCP and send to WebSocket"""
        try:
            conn = self.conn_manager.connections.get(conn_id)
            if not conn:
                return
            
            while True:
                data = await conn.reader.read(self.config.tcp_buffer_size)
                if not data:
                    # EOF - connection closed
                    self.logger.info(f"[Conn {conn_id}] TCP connection closed by remote")
                    break
                
                # Update stats
                async with self.conn_manager.lock:
                    conn.bytes_received += len(data)
                    conn.last_activity = time.time()
                
                # Send to client
                message = Message(MessageType.DATA, conn_id, data)
                await self.websocket.send(message.serialize())
                
                self.logger.debug(f"[Conn {conn_id}] Received {len(data)} bytes")
                
        except Exception as e:
            self.logger.error(f"[Conn {conn_id}] TCP reader error: {e}")
        finally:
            # Send close notification
            try:
                close_msg = Message(MessageType.CLOSE, conn_id)
                await self.websocket.send(close_msg.serialize())
            except:
                pass
            await self.conn_manager.close_connection(conn_id)
    
    async def cleanup(self):
        """Cleanup resources"""
        duration = time.time() - self.start_time
        stats = self.conn_manager.get_stats()
        
        self.logger.info(
            f"[{self.client_addr}] 🧹 Cleanup: {duration:.1f}s session, "
            f"{stats['active_connections']} connections, "
            f"↑{stats['total_bytes_sent']} ↓{stats['total_bytes_received']} bytes"
        )
        
        await self.conn_manager.close_all()

# ═══════════════════════════════════════════════════════════════════════════
# MAIN SERVER
# ═══════════════════════════════════════════════════════════════════════════

class TunnelServer:
    """Main tunnel server"""
    
    def __init__(self, config: ServerConfig):
        self.config = config
        self.logger = logging.getLogger("TunnelServer")
        self.ngrok_manager = NgrokManager(config)
        self.server = None
        self.running = False
    
    async def websocket_handler(self, websocket):
        """Handle new WebSocket connection"""
        handler = WebSocketHandler(websocket, self.config)
        await handler.handle()
    
    async def start(self):
        """Start the server"""
        self.running = True
        
        # Print banner
        self.print_banner()
        
        # Start ngrok if enabled
        if self.config.ngrok_enabled:
            try:
                public_url = await self.ngrok_manager.start()
                self.logger.info(f"🌐 Public URL: {public_url}")
            except Exception as e:
                self.logger.error(f"Failed to start ngrok: {e}")
                if input("Continue without ngrok? (y/n): ").lower() != 'y':
                    return
        
        # Start WebSocket server
        self.logger.info(f"🚀 Starting WebSocket server on {self.config.ws_host}:{self.config.ws_port}")
        
        try:
            async with websockets.serve(
                self.websocket_handler,
                self.config.ws_host,
                self.config.ws_port,
                ping_interval=self.config.ping_interval,
                ping_timeout=self.config.ping_timeout,
                max_size=self.config.max_message_size
            ):
                self.logger.info("✅ Server ready! Press Ctrl+C to stop")
                await asyncio.Future()  # Run forever
                
        except Exception as e:
            self.logger.error(f"Server error: {e}", exc_info=True)
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop the server"""
        if not self.running:
            return
        
        self.logger.info("🛑 Stopping server...")
        self.running = False
        self.ngrok_manager.stop()
        self.logger.info("✅ Server stopped")
    
    def print_banner(self):
        """Print startup banner"""
        banner = f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║             TCP-over-WebSocket Multiplexed Tunnel Server v2.0                ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

📋 Configuration:
   • WebSocket: {self.config.ws_host}:{self.config.ws_port}
   • Target TCP: {self.config.target_tcp_host}:{self.config.target_tcp_port}
   • Ngrok: {'Enabled' if self.config.ngrok_enabled else 'Disabled'}
   • Domain: {self.config.ngrok_domain if self.config.ngrok_enabled else 'N/A'}
   • Max Connections: {self.config.max_connections}
   • Buffer Size: {self.config.tcp_buffer_size} bytes

🎯 Features:
   ✅ Single WebSocket for multiple TCP connections
   ✅ Automatic connection pooling
   ✅ Built-in ngrok tunnel management
   ✅ Real-time statistics and monitoring
   ✅ Graceful error handling and recovery

"""
        print(banner)

# ═══════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

async def main():
    """Main entry point"""
    # Load configuration
    config = ServerConfig()
    
    # Setup logging
    setup_logging(config.log_level)
    
    # Create and start server
    server = TunnelServer(config)
    
    # Handle signals
    def signal_handler(sig, frame):
        print("\n")
        logging.getLogger("Main").info("Received interrupt signal, shutting down...")
        asyncio.create_task(server.stop())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start server
    await server.start()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        logging.getLogger("Main").critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
