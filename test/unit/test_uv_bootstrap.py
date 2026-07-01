from types import SimpleNamespace

from metaflow.plugins.uv.bootstrap import _extract_uv_binary


def test_extract_uv_binary_uses_filter_when_supported():
    calls = []

    class FilterCapableTar(object):
        def extractall(self, path, filter=None, members=None):
            calls.append((path, filter, members))

    tar = FilterCapableTar()
    marker = object()
    _extract_uv_binary(tar, "/tmp/uv", marker)

    assert len(calls) == 1
    assert calls[0] == ("/tmp/uv", marker, None)


def test_extract_uv_binary_falls_back_for_legacy_tarfile():
    calls = []
    members = [SimpleNamespace(name="nested/uv"), SimpleNamespace(name="nested/readme")]

    class LegacyTar(object):
        def extractall(self, path, filter=None, members=None):
            if filter is not None and members is None:
                raise TypeError(
                    "extractall() got an unexpected keyword argument 'filter'"
                )
            calls.append((path, filter, members))

        def getmembers(self):
            return members

    tar = LegacyTar()
    _extract_uv_binary(tar, "/tmp/uv", object())

    assert len(calls) == 1
    path, filter_arg, extracted_members = calls[0]
    assert path == "/tmp/uv"
    assert filter_arg is None
    assert len(extracted_members) == 1
    assert extracted_members[0].name == "uv"
