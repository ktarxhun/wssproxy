# 🚀 TCP-over-WebSocket Multiplexed Tunnel

**Production-ready tunnel system for TCP traffic over WebSocket connections**

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

## 📖 Overview

This tunnel system allows you to route TCP traffic through WebSocket connections, enabling connectivity through restrictive networks (like CGNAT) using ngrok or similar HTTP tunneling services.

### Key Features

✨ **Single WebSocket Connection** - Multiplexes multiple TCP connections over one WebSocket
🔄 **Auto-Reconnection** - Intelligent reconnection with exponential backoff
📊 **Connection Pooling** - Efficient management of multiple concurrent connections
🔌 **Built-in Ngrok** - Integrated ngrok tunnel management (server-side)
📈 **Real-time Stats** - Comprehensive logging and monitoring
🛡️ **Production-Ready** - Battle-tested error handling and recovery
⚡ **High Performance** - Optimized buffer management and async I/O

## 🏗️ Architecture

```
┌─────────────┐                                    ┌─────────────┐
│   Client    │                                    │   Server    │
│  Computer   │                                    │  Computer   │
├─────────────┤                                    ├─────────────┤
│             │                                    │             │
│ Application │ ─┐                            ┌─→  │ Application │
│ (connects   │  │                            │    │ (listens on │
│  to :1453)  │  │                            │    │   :1453)    │
│             │  │                            │    │             │
├─────────────┤  │                            │    ├─────────────┤
│             │  │                            │    │             │
│   Client    │  │  WebSocket (multiplexed)  │    │   Server    │
│   Tunnel    │  └──────────────────────────────┘  │   Tunnel    │
│             │                                    │             │
├─────────────┤                                    ├─────────────┤
│             │           ┌──────────┐             │             │
│             │ ────────→ │  Ngrok   │ ──────────→ │             │
│             │  HTTPS    │ (built-in)│   HTTP     │             │
└─────────────┘           └──────────┘             └─────────────┘
```

## 📦 Installation

### Requirements

```bash
# Python 3.8 or higher
python --version

# Install dependencies
pip install websockets aiohttp

# Install ngrok (server only)
# Download from: https://ngrok.com/download
# Or use package manager:
# - macOS: brew install ngrok
# - Windows: choco install ngrok
# - Linux: snap install ngrok
```

### Quick Start

1. **Download the files**
   - `tunnel_server_pro.py` - Server component (with ngrok)
   - `tunnel_client_pro.py` - Client component

2. **Configure ngrok domain** (if you have a custom domain)
   - Edit `tunnel_server_pro.py`, line ~35:
   ```python
   ngrok_domain: str = "your-domain.ngrok-free.app"
   ```

3. **Start the server** (on the computer with port 1453 service)
   ```bash
   python tunnel_server_pro.py
   ```

4. **Start the client** (on the computer that needs to connect to port 1453)
   ```bash
   python tunnel_client_pro.py
   ```

5. **Connect your application**
   ```bash
   # On client computer, connect to localhost:1453
   # Traffic will be tunneled to server's localhost:1453
   ```

## ⚙️ Configuration

### Server Configuration

Edit the `ServerConfig` class in `tunnel_server_pro.py`:

```python
@dataclass
class ServerConfig:
    # Network settings
    ws_host: str = "0.0.0.0"          # WebSocket bind address
    ws_port: int = 80                  # WebSocket port (ngrok connects here)
    target_tcp_host: str = "127.0.0.1" # Your application's host
    target_tcp_port: int = 1453        # Your application's port
    
    # Ngrok settings
    ngrok_domain: str = "your-domain.ngrok-free.app"
    ngrok_enabled: bool = True
    ngrok_region: str = "eu"  # eu, us, ap, au, sa, jp, in
    
    # Performance tuning
    max_message_size: int = 10 * 1024 * 1024  # 10MB
    tcp_buffer_size: int = 65536               # 64KB
    max_connections: int = 1000
    
    # Logging
    log_level: str = "INFO"  # DEBUG, INFO, WARNING, ERROR
```

### Client Configuration

Edit the `ClientConfig` class in `tunnel_client_pro.py`:

```python
@dataclass
class ClientConfig:
    # Network settings
    server_url: str = "wss://your-domain.ngrok-free.app"
    local_host: str = "127.0.0.1"
    local_port: int = 1453
    
    # Reconnection settings
    reconnect_enabled: bool = True
    reconnect_max_attempts: int = 0  # 0 = infinite
    reconnect_initial_delay: float = 1.0
    reconnect_max_delay: float = 60.0
    reconnect_backoff_factor: float = 2.0
    
    # Performance tuning
    max_message_size: int = 10 * 1024 * 1024
    tcp_buffer_size: int = 65536
    
    # Logging
    log_level: str = "INFO"
```

## 🧪 Testing

### Test with Echo Server

1. **Start test echo server** (included in previous version)
   ```bash
   python test_echo_server.py
   ```

2. **Start tunnel server**
   ```bash
   python tunnel_server_pro.py
   ```

3. **Start tunnel client** (on another computer or same computer)
   ```bash
   python tunnel_client_pro.py
   ```

4. **Test the connection**
   ```bash
   # Python test
   python
   >>> import socket
   >>> s = socket.socket()
   >>> s.connect(('localhost', 1453))
   >>> s.send(b'Hello World')
   >>> print(s.recv(1024))
   b'Hello World'
   >>> s.close()
   ```

   ```bash
   # Telnet test
   telnet localhost 1453
   
   # Netcat test
   echo "Hello" | nc localhost 1453
   ```

## 📊 Protocol Specification

### Message Format

All messages follow this binary format:

```
┌──────────┬──────────────┬───────────────┐
│ Type (1B)│ Conn ID (4B) │   Data (N)    │
└──────────┴──────────────┴───────────────┘
```

### Message Types

#### Client → Server

| Type | Code   | Description                    |
|------|--------|--------------------------------|
| CONNECT | 0x01 | Request new TCP connection     |
| DATA    | 0x02 | Send data through connection   |
| CLOSE   | 0x03 | Close TCP connection           |
| PING    | 0x04 | Keep-alive ping                |

#### Server → Client

| Type | Code   | Description                    |
|------|--------|--------------------------------|
| CONNECT_OK    | 0x81 | TCP connection successful |
| CONNECT_ERROR | 0x82 | TCP connection failed     |
| DATA          | 0x02 | Data from TCP connection  |
| CLOSE         | 0x03 | TCP connection closed     |
| PONG          | 0x84 | Keep-alive pong           |

## 🔧 Advanced Usage

### Custom Port Mapping

**Server side** - Map WebSocket port 80 to your SSH server on port 22:

```python
target_tcp_host: str = "127.0.0.1"
target_tcp_port: int = 22  # SSH port
```

**Client side** - Make SSH available on local port 2222:

```python
local_host: str = "127.0.0.1"
local_port: int = 2222
```

**Usage:**
```bash
ssh -p 2222 user@localhost
```

### Multiple Services

Run multiple client instances with different port mappings:

```bash
# Client 1 - SSH tunnel
python tunnel_client_pro.py  # configured for port 22

# Client 2 - RDP tunnel (modify config first)
python tunnel_client_pro.py  # configured for port 3389
```

### Disable Ngrok

If you're using a different HTTP tunnel service:

**Server:**
```python
ngrok_enabled: bool = False
```

Then use your own tunnel (cloudflared, localtunnel, etc.) to expose port 80.

### Debug Logging

Enable detailed logging:

```python
log_level: str = "DEBUG"
```

This will show every message sent/received and all state changes.

## 🐛 Troubleshooting

### Connection Refused

**Symptom:** `TCP connection refused` error on server

**Solution:**
- Check if your target application is running
- Verify `target_tcp_port` is correct
- Ensure application is listening on `target_tcp_host`

### WebSocket Connection Failed

**Symptom:** Client can't connect to WebSocket

**Solution:**
- Verify server is running
- Check ngrok is active: `http://127.0.0.1:4040`
- Confirm `server_url` in client matches ngrok URL
- Check firewall rules

### High Latency

**Symptom:** Slow response times

**Solution:**
- Choose ngrok region closer to you
- Increase `tcp_buffer_size` for bulk transfers
- Check network connection quality
- Consider paid ngrok plan for better performance

### Connection Drops

**Symptom:** Connections randomly disconnect

**Solution:**
- Check `ping_interval` and `ping_timeout` settings
- Enable `reconnect_enabled` on client
- Monitor ngrok connection stability
- Check for network interruptions

### Ngrok Not Found

**Symptom:** `Ngrok not found` error

**Solution:**
```bash
# Install ngrok
# Visit: https://ngrok.com/download

# Verify installation
ngrok version

# Add to PATH if needed (Linux/Mac)
export PATH=$PATH:/path/to/ngrok

# Windows: Add ngrok directory to System PATH
```

## 📈 Performance Tuning

### High Throughput

For bulk data transfer:

```python
tcp_buffer_size: int = 131072  # 128KB
max_message_size: int = 50 * 1024 * 1024  # 50MB
```

### Many Connections

For services with many concurrent connections:

```python
max_connections: int = 5000
connection_timeout: int = 600  # 10 minutes
```

### Low Latency

For real-time applications:

```python
tcp_buffer_size: int = 8192  # 8KB
ping_interval: int = 10
```

## 🔒 Security Considerations

⚠️ **Important:** This tunnel provides **NO encryption** beyond what ngrok/WebSocket provides.

### Recommendations:

1. **Use HTTPS/WSS** - Ngrok provides this by default
2. **Application-level encryption** - Use SSH, TLS, or VPN over the tunnel
3. **Access control** - Restrict ngrok URL sharing
4. **Monitor connections** - Review logs regularly
5. **Firewall rules** - Limit who can connect to local ports

### Production Deployment:

- Use ngrok authentication (`--auth` flag)
- Consider ngrok paid plans for IP whitelisting
- Implement rate limiting if needed
- Set up monitoring and alerting
- Use HTTPS-only connections

## 📝 License

MIT License - See LICENSE file for details

## 🤝 Contributing

Contributions welcome! Please follow these guidelines:

1. Test your changes thoroughly
2. Update documentation
3. Follow existing code style
4. Add logging for new features
5. Handle errors gracefully

## 💡 Tips & Best Practices

1. **Start server before client** - Client will wait but better to have server ready
2. **Use DEBUG logging** - When troubleshooting issues
3. **Monitor ngrok dashboard** - http://127.0.0.1:4040 for traffic analysis
4. **Test locally first** - Before deploying across networks
5. **Keep connections alive** - Applications should handle reconnections
6. **Monitor resource usage** - Check CPU/memory on high traffic

## 📞 Support

For issues and questions:
- Review logs with DEBUG level
- Check ngrok dashboard for connectivity issues
- Verify port configurations
- Test with simple echo server first

## 🎯 Use Cases

- **CGNAT Bypass** - Connect devices behind CGNAT
- **Development Testing** - Test local services from remote locations
- **IoT Connectivity** - Connect IoT devices without public IPs
- **Remote Access** - Access home services from anywhere
- **Service Tunneling** - Expose any TCP service through HTTP tunnels

---

**Built with ❤️ using Python and WebSockets**

Version 2.0.0 | Last Updated: 2026-02-15
