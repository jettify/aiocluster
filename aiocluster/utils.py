import datetime
import struct


def utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


def decode_msg_size(raw_payload: bytes) -> int:
    s = raw_payload[:4]
    m_size: int
    m_size, *_ = struct.unpack(">I", s)
    return m_size


def add_msg_size(raw_payload: bytes) -> bytes:
    m_size = len(raw_payload)
    fmt = f">I{m_size}s"
    raw_payload = struct.pack(fmt, m_size, raw_payload)
    return raw_payload
