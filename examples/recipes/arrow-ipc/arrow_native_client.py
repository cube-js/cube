#!/usr/bin/env python3
"""
Arrow Native Protocol Client for CubeSQL

Implements the custom Arrow Native protocol (port 4445) for CubeSQL.
This protocol wraps Arrow IPC data in a custom message format.

Protocol Messages:
- HandshakeRequest/Response: Protocol version negotiation
- AuthRequest/Response: Authentication with token
- QueryRequest: SQL query execution
- QueryResponseSchema: Arrow IPC schema bytes
- QueryResponseBatch: Arrow IPC batch bytes (can be multiple)
- QueryComplete: Query finished

Message Format:
- All messages start with: u8 message_type
- Strings encoded as: u32 length + utf-8 bytes
- Arrow IPC data: raw bytes (schema or batch)
"""

import socket
import struct
from typing import List, Optional, Tuple
from dataclasses import dataclass
import pyarrow as pa
import pyarrow.ipc as ipc
import io


class MessageType:
    """Message type constants matching Rust protocol.rs"""
    HANDSHAKE_REQUEST = 0x01
    HANDSHAKE_RESPONSE = 0x02
    AUTH_REQUEST = 0x03
    AUTH_RESPONSE = 0x04
    QUERY_REQUEST = 0x10
    QUERY_RESPONSE_SCHEMA = 0x11
    QUERY_RESPONSE_BATCH = 0x12
    QUERY_COMPLETE = 0x13
    ERROR = 0xFF


@dataclass
class QueryResult:
    """Result from Arrow Native query execution"""
    schema: pa.Schema
    batches: List[pa.RecordBatch]
    rows_affected: int

    def to_table(self) -> pa.Table:
        """Convert batches to PyArrow Table"""
        if not self.batches:
            return pa.Table.from_pydict({}, schema=self.schema)
        return pa.Table.from_batches(self.batches, schema=self.schema)

    def to_pandas(self):
        """Convert to pandas DataFrame"""
        return self.to_table().to_pandas()


class ArrowNativeClient:
    """Client for CubeSQL Arrow Native protocol (port 4445)"""

    PROTOCOL_VERSION = 1

    def __init__(self, host: str = "localhost", port: int = 4445,
                 token: str = "test", database: Optional[str] = None):
        self.host = host
        self.port = port
        self.token = token
        self.database = database
        self.socket: Optional[socket.socket] = None
        self.session_id: Optional[str] = None

    def connect(self):
        """Connect and authenticate to Arrow Native server"""
        # Create socket connection
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))

        # Handshake
        self._send_handshake()
        server_version = self._receive_handshake()

        # Authentication
        self._send_auth()
        self.session_id = self._receive_auth()

        return self

    def close(self):
        """Close connection"""
        if self.socket:
            self.socket.close()
            self.socket = None

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def query(self, sql: str) -> QueryResult:
        """Execute SQL query and return Arrow result"""
        if not self.socket:
            raise RuntimeError("Not connected - call connect() first")

        # Send query request
        self._send_query(sql)

        # Receive schema
        schema = self._receive_schema()

        # Receive batches
        batches = []
        while True:
            payload = self._receive_message()
            msg_type = payload[0]

            if msg_type == MessageType.QUERY_RESPONSE_BATCH:
                batch = self._receive_batch(schema, payload)
                batches.append(batch)
            elif msg_type == MessageType.QUERY_COMPLETE:
                rows_affected = struct.unpack('>q', payload[1:9])[0]
                break
            elif msg_type == MessageType.ERROR:
                # Parse error
                code_len = struct.unpack('>I', payload[1:5])[0]
                code = payload[5:5+code_len].decode('utf-8')
                msg_len = struct.unpack('>I', payload[5+code_len:9+code_len])[0]
                message = payload[9+code_len:9+code_len+msg_len].decode('utf-8')
                raise RuntimeError(f"Query error [{code}]: {message}")
            else:
                raise RuntimeError(f"Unexpected message type: 0x{msg_type:02x}")

        return QueryResult(schema=schema, batches=batches, rows_affected=rows_affected)

    # === Handshake ===

    def _send_handshake(self):
        """Send HandshakeRequest"""
        payload = bytearray()
        payload.append(MessageType.HANDSHAKE_REQUEST)
        payload.extend(struct.pack('>I', self.PROTOCOL_VERSION))
        self._send_message(payload)

    def _receive_handshake(self) -> str:
        """Receive HandshakeResponse"""
        payload = self._receive_message()
        if payload[0] != MessageType.HANDSHAKE_RESPONSE:
            raise RuntimeError(f"Expected HandshakeResponse, got 0x{payload[0]:02x}")

        # Parse payload
        version = struct.unpack('>I', payload[1:5])[0]
        if version != self.PROTOCOL_VERSION:
            raise RuntimeError(f"Protocol version mismatch: client={self.PROTOCOL_VERSION}, server={version}")

        # Read server version string
        str_len = struct.unpack('>I', payload[5:9])[0]
        server_version = payload[9:9+str_len].decode('utf-8')
        return server_version

    def _receive_message(self) -> bytes:
        """Receive a length-prefixed message"""
        # Read length prefix
        length = self._read_u32()
        if length == 0 or length > 100 * 1024 * 1024:  # 100MB max
            raise RuntimeError(f"Invalid message length: {length}")
        # Read payload
        return self._read_exact(length)

    # === Authentication ===

    def _send_auth(self):
        """Send AuthRequest"""
        payload = bytearray()
        payload.append(MessageType.AUTH_REQUEST)
        payload.extend(self._encode_string(self.token))
        payload.extend(self._encode_optional_string(self.database))
        self._send_message(payload)

    def _receive_auth(self) -> str:
        """Receive AuthResponse"""
        payload = self._receive_message()
        if payload[0] != MessageType.AUTH_RESPONSE:
            raise RuntimeError(f"Expected AuthResponse, got 0x{payload[0]:02x}")

        success = payload[1] != 0
        # Read session_id string
        str_len = struct.unpack('>I', payload[2:6])[0]
        session_id = payload[6:6+str_len].decode('utf-8')

        if not success:
            raise RuntimeError(f"Authentication failed: {session_id}")

        return session_id

    # === Query ===

    def _send_query(self, sql: str):
        """Send QueryRequest"""
        payload = bytearray()
        payload.append(MessageType.QUERY_REQUEST)
        payload.extend(self._encode_string(sql))
        self._send_message(payload)

    def _send_message(self, payload: bytes):
        """Send a length-prefixed message"""
        # Prepend u32 length
        length = struct.pack('>I', len(payload))
        self.socket.sendall(length + payload)

    def _receive_schema(self) -> pa.Schema:
        """Receive QueryResponseSchema"""
        payload = self._receive_message()

        if payload[0] == MessageType.ERROR:
            # Parse error message
            code_len = struct.unpack('>I', payload[1:5])[0]
            code = payload[5:5+code_len].decode('utf-8')
            msg_len = struct.unpack('>I', payload[5+code_len:9+code_len])[0]
            message = payload[9+code_len:9+code_len+msg_len].decode('utf-8')
            raise RuntimeError(f"Query error [{code}]: {message}")

        if payload[0] != MessageType.QUERY_RESPONSE_SCHEMA:
            raise RuntimeError(f"Expected QueryResponseSchema, got 0x{payload[0]:02x}")

        # Extract Arrow IPC schema bytes (after message type and length prefix)
        schema_len = struct.unpack('>I', payload[1:5])[0]
        schema_bytes = payload[5:5+schema_len]

        # Decode Arrow IPC schema
        reader = ipc.open_stream(io.BytesIO(schema_bytes))
        return reader.schema

    def _receive_batch(self, schema: pa.Schema, payload: bytes) -> pa.RecordBatch:
        """Receive QueryResponseBatch (payload already read)"""
        # Extract Arrow IPC batch bytes (after message type and length prefix)
        batch_len = struct.unpack('>I', payload[1:5])[0]
        batch_bytes = payload[5:5+batch_len]

        # Decode Arrow IPC batch
        reader = ipc.open_stream(io.BytesIO(batch_bytes))
        batch = reader.read_next_batch()
        return batch

    # === Low-level I/O ===

    def _read_u8(self) -> int:
        """Read unsigned 8-bit integer"""
        data = self.socket.recv(1)
        if len(data) != 1:
            raise RuntimeError("Connection closed")
        return data[0]

    def _read_bool(self) -> bool:
        """Read boolean (u8)"""
        return self._read_u8() != 0

    def _read_exact(self, n: int) -> bytes:
        """Read exactly n bytes from socket (handles partial reads)"""
        data = bytearray()
        while len(data) < n:
            chunk = self.socket.recv(n - len(data))
            if not chunk:
                raise RuntimeError("Connection closed")
            data.extend(chunk)
        return bytes(data)

    def _read_u32(self) -> int:
        """Read unsigned 32-bit integer (big-endian)"""
        data = self._read_exact(4)
        return struct.unpack('>I', data)[0]

    def _read_i64(self) -> int:
        """Read signed 64-bit integer (big-endian)"""
        data = self._read_exact(8)
        return struct.unpack('>q', data)[0]

    def _read_string(self) -> str:
        """Read length-prefixed UTF-8 string"""
        length = self._read_u32()
        if length == 0:
            return ""
        data = self._read_exact(length)
        return data.decode('utf-8')

    def _read_bytes(self) -> bytes:
        """Read length-prefixed byte array"""
        length = self._read_u32()
        if length == 0:
            return b""
        data = self._read_exact(length)
        return data

    def _encode_string(self, s: str) -> bytes:
        """Encode string as length-prefixed UTF-8"""
        utf8_bytes = s.encode('utf-8')
        return struct.pack('>I', len(utf8_bytes)) + utf8_bytes

    def _encode_optional_string(self, s: Optional[str]) -> bytes:
        """Encode optional string (bool present + string if present)"""
        if s is None:
            return struct.pack('B', 0)  # false
        else:
            return struct.pack('B', 1) + self._encode_string(s)  # true + string


# Example usage
if __name__ == "__main__":
    import time

    print("Testing Arrow Native Client")
    print("=" * 60)

    with ArrowNativeClient(host="localhost", port=4445, token="test") as client:
        print(f"✓ Connected (session: {client.session_id})")

        # Test query
        sql = "SELECT 1 as num, 'hello' as text"
        print(f"\nQuery: {sql}")

        start = time.time()
        result = client.query(sql)
        elapsed_ms = int((time.time() - start) * 1000)

        print(f"✓ Received {len(result.batches)} batches")
        print(f"✓ Schema: {result.schema}")
        print(f"✓ Time: {elapsed_ms}ms")

        # Convert to pandas
        df = result.to_pandas()
        print(f"\nResult ({len(df)} rows):")
        print(df)

    print("\n✓ Connection closed")
