"""DJB-variant hash function for deterministic bucketing.

Must produce identical results to the TypeScript implementation:
    function hashString(str: string): number {
        let hash = 0;
        for (let i = 0; i < str.length; i++) {
            const char = str.charCodeAt(i);
            hash = (hash << 5) - hash + char;
            hash |= 0;
        }
        return Math.abs(hash);
    }
"""

import ctypes


def hash_string(s: str) -> int:
    """Hash a string using DJB-variant with 32-bit signed integer arithmetic.

    Returns the absolute value of the hash.
    """
    h = ctypes.c_int32(0)
    for ch in s:
        code = ord(ch)
        # (hash << 5) - hash + char, truncated to 32-bit signed int
        h = ctypes.c_int32((h.value << 5) - h.value + code)
    return abs(h.value)
