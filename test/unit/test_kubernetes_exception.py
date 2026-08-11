def test_kubernetes_exception_is_single_class():
    """KubernetesException must resolve to one class regardless of import path.

    It was previously defined separately in both
    ``metaflow/plugins/kubernetes/kubernetes.py`` and
    ``metaflow/plugins/kubernetes/kube_utils.py``. Because different modules
    imported it from different places (``kubernetes_decorator`` from
    ``kubernetes``, ``kubernetes_cli`` from ``kube_utils``), an
    ``except KubernetesException`` on one path would not catch a raise of the
    class from the other. Both import paths must be the same object.
    """
    from metaflow.plugins.kubernetes.kubernetes import (
        KubernetesException as FromKubernetes,
    )
    from metaflow.plugins.kubernetes.kube_utils import (
        KubernetesException as FromKubeUtils,
    )

    assert FromKubernetes is FromKubeUtils

    # And an except-clause using one path catches a raise from the other.
    try:
        raise FromKubeUtils("boom")
    except FromKubernetes:
        caught = True
    assert caught
