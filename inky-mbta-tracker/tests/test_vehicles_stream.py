from zlib_ng import zlib_ng

import pytest


def _compress_single_stream(chunks: list[bytes]) -> bytes:
    compressor = zlib_ng.compressobj(wbits=zlib_ng.MAX_WBITS + 16)
    result = b""
    for chunk in chunks:
        result += compressor.compress(chunk)
        result += compressor.flush(zlib_ng.Z_SYNC_FLUSH)
    return result


@pytest.mark.anyio
async def test_single_gzip_stream_decompresses_incrementally() -> None:
    chunks = [b"hello ", b"world"]
    compressed = _compress_single_stream(chunks)

    decompressor = zlib_ng.decompressobj(wbits=zlib_ng.MAX_WBITS + 16)
    output = b""
    chunk_size = 8
    for i in range(0, len(compressed), chunk_size):
        output += decompressor.decompress(compressed[i : i + chunk_size])
    output += decompressor.flush()

    assert output == b"".join(chunks)


@pytest.mark.anyio
async def test_single_gzip_stream_with_sse_frames() -> None:
    frames = [
        b": stream-start\n\n",
        b'data: {"updated": {}, "removed": []}\n\n',
        b'data: {"updated": {"v1": {}}, "removed": []}\n\n',
    ]
    compressed = _compress_single_stream(frames)

    decompressor = zlib_ng.decompressobj(wbits=zlib_ng.MAX_WBITS + 16)
    output = decompressor.decompress(compressed)
    output += decompressor.flush()

    assert output == b"".join(frames)


@pytest.mark.anyio
async def test_single_gzip_stream_is_one_member() -> None:
    chunks = [b"part1\n\n", b"part2\n\n"]
    compressed = _compress_single_stream(chunks)

    decompressor = zlib_ng.decompressobj(wbits=zlib_ng.MAX_WBITS + 16)
    output = decompressor.decompress(compressed)
    output += decompressor.flush()

    assert output == b"part1\n\npart2\n\n"
