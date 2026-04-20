from metaflow.packaging_sys import ContentType
from metaflow.packaging_sys.v1 import MetaflowCodeContentV1


def test_include_true_yields_metaflow_code():
    c = MetaflowCodeContentV1(include_mf_distribution=True)
    names = [n for _, n in c.content_names(ContentType.CODE_CONTENT.value)]
    assert any(n.endswith("metaflow/__init__.py") for n in names)


def test_include_false_skips_metaflow_code():
    c = MetaflowCodeContentV1(include_mf_distribution=False)
    names = [n for _, n in c.content_names(ContentType.CODE_CONTENT.value)]
    assert not any(n.endswith("metaflow/__init__.py") for n in names)
    # INFO/CONFIG markers (OTHER_CONTENT) should still be produced.
    assert list(c.content_names(ContentType.OTHER_CONTENT.value))
