"""Memory-friendly Termux variant (stdlib only, fully streaming)."""
from __future__ import annotations

import hashlib
import inspect
from pathlib import Path

import pytest

from termux import run_parallel_lite as lite
from tests.workers import square


@pytest.mark.parametrize("backend", ["thread", "serial", "process"])
def test_lite_backends(backend, numbers):
    runner = lite.LiteParallel(workers=2, backend=backend)
    result = runner.run(square, numbers)
    assert result.unwrap() == [x * x for x in numbers]
    assert result.ok and result.submitted == len(numbers)
    assert result.elapsed >= 0
    assert result.peak_rss_kb is None or result.peak_rss_kb > 0


def test_imap_is_a_generator_with_bounded_window(numbers):
    runner = lite.LiteParallel(workers=2, max_pending=3, backend="thread")
    assert runner.max_pending == 3
    stream = runner.imap(square, iter(numbers))
    assert inspect.isgenerator(stream)
    assert sorted(outcome.value for outcome in stream) == sorted(
        x * x for x in numbers
    )
    assert all(outcome.ok for outcome in runner.imap(square, numbers))


def test_map_yields_values_only(numbers):
    assert sorted(lite.LiteParallel(workers=2).map(square, numbers)) == sorted(
        x * x for x in numbers
    )


def test_error_policies():
    def maybe_fail(value):
        if value == 3:
            raise ValueError("no 3")
        return value * 10

    collected = lite.LiteParallel(workers=2, on_error="collect").run(
        maybe_fail, [1, 2, 3, 4]
    )
    assert len(collected.values) == 3
    assert len(collected.errors) == 1
    assert "no 3" in str(collected.errors[0].error)
    assert not collected.ok
    # Worker errors are normalised to RuntimeError("OriginalType: message"),
    # so no traceback frames (and their buffers) are retained or pickled.
    with pytest.raises(RuntimeError, match="no 3"):
        collected.unwrap()

    skipped = lite.LiteParallel(workers=2, on_error="skip").run(
        maybe_fail, [1, 2, 3, 4]
    )
    assert skipped.values == [10, 20, 40] and skipped.errors == []

    with pytest.raises(RuntimeError, match="no 3"):
        lite.LiteParallel(workers=2, on_error="raise").run(maybe_fail, [1, 2, 3])


def test_invalid_configuration():
    with pytest.raises(ValueError):
        lite.LiteParallel(backend="gpu")
    with pytest.raises(ValueError):
        lite.LiteParallel(on_error="ignore")
    with pytest.raises(ValueError):
        lite.LiteParallel(timeout=0)


def test_iter_files_streams_and_respects_limit(tmp_path):
    (tmp_path / "sub").mkdir()
    for index in range(6):
        (tmp_path / "sub" / f"f{index}.bin").write_bytes(bytes([index]) * 16)
    (tmp_path / ".hidden").mkdir()
    (tmp_path / ".hidden" / "secret.bin").write_bytes(b"x")

    stream = lite.iter_files(tmp_path, limit=4)
    assert inspect.isgenerator(stream)
    paths = list(stream)
    assert len(paths) == 4
    assert all(not Path(p).name.startswith(".") for p in paths)
    assert len(list(lite.iter_files(tmp_path, include_hidden=True))) >= 7


def test_hash_file_block_size_does_not_change_digest(tmp_path):
    path = tmp_path / "blob.bin"
    payload = bytes(range(256)) * 4096  # 1 MiB
    path.write_bytes(payload)
    small = lite.hash_file(str(path), block_size=1024)
    big = lite.hash_file(str(path), block_size=1 << 20)
    assert small == big
    assert small[1] == hashlib.sha256(payload).hexdigest()
    assert small[2] == len(payload)


def test_hash_folder_is_fully_streaming_and_correct(tmp_path, small_files):
    stream = lite.hash_folder(tmp_path, workers=2, max_pending=4, block_size=4096)
    assert inspect.isgenerator(stream)
    outcomes = list(stream)
    assert len(outcomes) == len(small_files)
    assert all(outcome.ok for outcome in outcomes)
    got = {(value[0], value[1]) for outcome in outcomes for value in [outcome.value]}
    expected = {
        (path, hashlib.sha256(Path(path).read_bytes()).hexdigest())
        for path in small_files
    }
    assert got == expected


def test_iter_file_digests_serial(tmp_path, small_files):
    digests = list(lite.iter_file_digests(tmp_path))
    assert len(digests) == len(small_files)
    assert all(len(digest) == 64 for _, digest, _ in digests)


def test_cli_main_prints_digests_and_summary(tmp_path, capsys):
    (tmp_path / "a.bin").write_bytes(b"a" * 100)
    code = lite.main([str(tmp_path), "--workers", "2", "--block-size", "32",
                      "--gc-every", "1", "--algorithm", "sha256"])
    assert code == 0
    out = capsys.readouterr()
    assert hashlib.sha256(b"a" * 100).hexdigest() in out.out
    assert "files," in out.err and "peak RSS" in out.err


def test_default_folder_is_home(monkeypatch, tmp_path):
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    (tmp_path / "home.bin").write_bytes(b"home")
    assert [Path(p) for p in lite.iter_files()] == [tmp_path / "home.bin"]
