from .sidecar_subprocess import SidecarSubProcess


class Sidecar(object):
    def __init__(self, sidecar_type):
        # Needs to be here because this file gets loaded by lots of things and SIDECARS
        # may not be fully populated by then
        from metaflow.plugins import SIDECARS

        self._sidecar_type = sidecar_type
        self._has_valid_worker = False
        t = SIDECARS.get(self._sidecar_type)
        if t is not None and t.get_worker() is not None:
            self._has_valid_worker = True
        self.sidecar_process = None

    def start(self):
        if not self.is_active and self._has_valid_worker:
            self.sidecar_process = SidecarSubProcess(self._sidecar_type)

    def send(self, msg):
        if self.is_active:
            self.sidecar_process.send(msg)

    def terminate(self):
        if self.is_active:
            self.sidecar_process.kill()

    @property
    def is_active(self):
        return self.sidecar_process is not None
