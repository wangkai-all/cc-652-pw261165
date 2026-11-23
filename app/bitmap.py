"""位图抽象层。

优先使用 pyroaring 的压缩位图 (Roaring Bitmap)，磁盘格式带魔数与版本；
当环境中没有 pyroaring 时，退化为基于 Python 大整数的位图实现，
两种磁盘格式互不兼容，魔数不同，读取时按魔数分派。

磁盘布局::

    magic(1) version(1) payload...

- magic 0x52 ('R'): Roaring 序列化负载
- magic 0x50 ('P'): 纯 Python int，负载为大端字节长度(uint32) + 无符号大整数字节
"""
from __future__ import annotations

import struct
from typing import Iterable, Iterator

try:  # pragma: no cover - 环境相关分支
    from pyroaring import BitMap as _Roaring

    HAVE_ROARING = True
except Exception:  # pragma: no cover
    _Roaring = None
    HAVE_ROARING = False

MAGIC_ROARING = 0x52
MAGIC_PYINT = 0x50
VERSION = 1


class Bitmap:
    """统一位图接口，屏蔽底层实现。"""

    __slots__ = ("_impl",)

    def __init__(self, values: Iterable[int] | None = None) -> None:
        if HAVE_ROARING:
            self._impl = _Roaring(values) if values is not None else _Roaring()
        else:
            self._impl = _PyIntBitmap(values)

    # ---- 集合运算（返回新位图）-----------------------------------------
    def and_(self, other: "Bitmap") -> "Bitmap":
        out = Bitmap.__new__(Bitmap)
        out._impl = self._impl & other._impl
        return out

    def or_(self, other: "Bitmap") -> "Bitmap":
        out = Bitmap.__new__(Bitmap)
        out._impl = self._impl | other._impl
        return out

    def andnot(self, other: "Bitmap") -> "Bitmap":
        out = Bitmap.__new__(Bitmap)
        out._impl = self._impl - other._impl
        return out

    def __and__(self, other: "Bitmap") -> "Bitmap":
        return self.and_(other)

    def __or__(self, other: "Bitmap") -> "Bitmap":
        return self.or_(other)

    def __sub__(self, other: "Bitmap") -> "Bitmap":
        return self.andnot(other)

    # ---- 单元素 / 容器语义 ---------------------------------------------
    def add(self, value: int) -> None:
        self._impl.add(value)

    def contains(self, value: int) -> bool:
        return value in self._impl

    def __contains__(self, value: int) -> bool:
        return value in self._impl

    def __len__(self) -> int:
        return len(self._impl)

    def __iter__(self) -> Iterator[int]:
        return iter(self._impl)

    def to_list(self) -> list[int]:
        return list(self._impl)

    def copy(self) -> "Bitmap":
        out = Bitmap.__new__(Bitmap)
        out._impl = self._impl.copy()
        return out

    @property
    def is_roaring(self) -> bool:
        return HAVE_ROARING

    # ---- 持久化 ---------------------------------------------------------
    def serialize(self) -> bytes:
        if HAVE_ROARING:
            return bytes([MAGIC_ROARING, VERSION]) + bytes(self._impl.serialize())
        payload = int(self._impl).to_bytes((int(self._impl).bit_length() + 7) // 8 or 1, "big")
        return bytes([MAGIC_PYINT, VERSION]) + struct.pack(">I", len(payload)) + payload

    @classmethod
    def deserialize(cls, raw: bytes) -> "Bitmap":
        if len(raw) < 2:
            raise ValueError("位图文件过短，格式非法")
        magic, ver = raw[0], raw[1]
        if ver != VERSION:
            raise ValueError(f"不支持的位图版本: {ver}")
        out = cls.__new__(cls)
        if magic == MAGIC_ROARING:
            if not HAVE_ROARING:
                raise ValueError("该位图为 Roaring 格式，但当前环境没有 pyroaring，无法读取")
            out._impl = _Roaring.deserialize(raw[2:])
        elif magic == MAGIC_PYINT:
            (n,) = struct.unpack(">I", raw[2:6])
            out._impl = _PyIntBitmap.from_int(int.from_bytes(raw[6 : 6 + n], "big"))
        else:
            raise ValueError(f"无法识别的位图魔数: 0x{magic:02x}（疑似损坏或伪造文件）")
        return out


class _PyIntBitmap:
    """无 pyroaring 时的回退实现：一个大整数即一张位图。"""

    __slots__ = ("_bits",)

    def __init__(self, values: Iterable[int] | None = None) -> None:
        bits = 0
        if values is not None:
            for v in values:
                if v < 0:
                    raise ValueError("行号必须非负")
                bits |= 1 << v
        self._bits = bits

    @classmethod
    def from_int(cls, n: int) -> "_PyIntBitmap":
        obj = cls()
        obj._bits = n
        return obj

    def __int__(self) -> int:
        return self._bits

    def add(self, value: int) -> None:
        if value < 0:
            raise ValueError("行号必须非负")
        self._bits |= 1 << value

    def __contains__(self, value: int) -> bool:
        return bool(self._bits & (1 << value))

    def __len__(self) -> int:
        return self._bits.bit_count()

    def __iter__(self) -> Iterator[int]:
        b = self._bits
        while b:
            low = b & -b
            yield low.bit_length() - 1
            b ^= low

    def __and__(self, other: "_PyIntBitmap") -> "_PyIntBitmap":
        return _PyIntBitmap.from_int(self._bits & other._bits)

    def __or__(self, other: "_PyIntBitmap") -> "_PyIntBitmap":
        return _PyIntBitmap.from_int(self._bits | other._bits)

    def __sub__(self, other: "_PyIntBitmap") -> "_PyIntBitmap":
        return _PyIntBitmap.from_int(self._bits & ~other._bits)

    def andnot(self, other: "_PyIntBitmap") -> "_PyIntBitmap":
        return self.__sub__(other)

    def copy(self) -> "_PyIntBitmap":
        return _PyIntBitmap.from_int(self._bits)
