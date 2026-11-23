"""位图适配层：Roaring 与纯 Python 回退的序列化、互读拒用。"""
import importlib
import sys

import pytest


def _reload_bitmap(have_roaring: bool):
    """强制切换位图后端重新导入。"""
    sys.modules.pop("app.bitmap", None)
    if not have_roaring:
        # 屏蔽 pyroaring，强制走 _PyIntBitmap 回退
        sys.modules["pyroaring"] = None  # type: ignore[assignment]
    else:
        sys.modules.pop("pyroaring", None)
    mod = importlib.import_module("app.bitmap")
    return importlib.reload(mod)


def test_roaring_roundtrip_and_set_ops():
    bm = _reload_bitmap(True)
    assert bm.HAVE_ROARING is True
    a = bm.Bitmap([1, 2, 3, 100])
    b = bm.Bitmap([2, 3, 5])
    assert sorted(a & b) == [2, 3]
    assert sorted(a | b) == [1, 2, 3, 5, 100]
    assert sorted(a - b) == [1, 100]
    raw = a.serialize()
    assert raw[0] == bm.MAGIC_ROARING
    back = bm.Bitmap.deserialize(raw)
    assert sorted(back) == [1, 2, 3, 100]


def test_pyint_fallback_roundtrip_and_set_ops():
    bm = _reload_bitmap(False)
    assert bm.HAVE_ROARING is False
    a = bm.Bitmap([1, 2, 3, 100])
    b = bm.Bitmap([2, 3, 5])
    assert sorted(a & b) == [2, 3]
    assert sorted(a | b) == [1, 2, 3, 5, 100]
    assert sorted(a - b) == [1, 100]
    a.add(50)
    assert 50 in a and 999 not in a
    raw = a.serialize()
    assert raw[0] == bm.MAGIC_PYINT
    back = bm.Bitmap.deserialize(raw)
    assert sorted(back) == [1, 2, 3, 50, 100]
    # 恢复真实环境
    _reload_bitmap(True)


def test_cross_format_and_garbage_are_rejected():
    roaring = _reload_bitmap(True)
    pyint = _reload_bitmap(False)
    py_raw = pyint.Bitmap([1, 2]).serialize()
    _reload_bitmap(True)
    # Roaring 环境下读到 PyInt 格式：按魔数分派，本实现两种都能读（仅后端不同）
    back = roaring.Bitmap.deserialize(py_raw)
    assert sorted(back) == [1, 2]
    # 损坏魔数 / 过短文件必须显式报错，而不是静默返回空结果
    with pytest.raises(ValueError, match="魔数|格式"):
        roaring.Bitmap.deserialize(b"\x99\x01\x00\x00")
    with pytest.raises(ValueError, match="过短"):
        roaring.Bitmap.deserialize(b"\x52")
    with pytest.raises(ValueError):
        roaring.Bitmap.deserialize(b"")
    _reload_bitmap(True)


def test_negative_ordinals_rejected():
    bm = _reload_bitmap(False)
    with pytest.raises(ValueError):
        bm.Bitmap([-1])
    _reload_bitmap(True)
