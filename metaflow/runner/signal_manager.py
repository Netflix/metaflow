import asyncio
import signal
from typing import NewType, Mapping, Set, Callable, Optional

SignalHandler = NewType("SignalHandler", Callable[[int, []], None])


class SignalManager:
    """
    A context manager for managing signal handlers.

    This class works as a context manager, restoring any overwritten
    signal handlers when the context is exited. This only works for signals
    in a synchronous context (ie. hooked by `signal`).

    Parameters
    ----------
    hook_signals : bool
        If True, the signal manager will overwrite any existing signal handlers
        in either `asyncio` or `signal`. If you already have any signal
        handling in place, you can set this to False and use `trigger_signal`
        to trigger metaflow-related signal handlers.
    event_loop : Optional[asyncio.AbstractEventLoop]
        The event loop to use for handling signals.
        If None, the current running event loop is used, if any.
    """

    hook_signals: bool
    event_loop: Optional[asyncio.AbstractEventLoop]
    signal_map: Mapping[int, Set[SignalHandler]] = dict()
    replaced_signals: Mapping[int, SignalHandler] = dict()

    def __init__(
        self,
        hook_signals: bool = True,
        event_loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        self.hook_signals = hook_signals
        try:
            self.event_loop = event_loop or asyncio.get_running_loop()
        except RuntimeError:
            self.event_loop = None

    def __exit__(self, exc_type, exc_value, traceback):
        for sig in self.signal_map:
            self._maybe_remove_signal_handler(sig)

        for sig in self.replaced_signals:
            signal.signal(sig, self.replaced_signals[sig])

    def _handle_signal(self, signum, frame):
        for handler in self.signal_map[signum]:
            handler(signum, frame)

    def _maybe_add_signal_handler(self, sig):
        if not self.hook_signals:
            return

        if self.event_loop is None:
            replaced = signal.signal(sig, self._handle_signal)
            self.replaced_signals[sig] = replaced

        else:
            self.event_loop.add_signal_handler(
                sig, lambda: self._handle_signal(sig, None)
            )

    def _maybe_remove_signal_handler(self, sig: int):
        if not self.hook_signals:
            return

        if self.event_loop is None:
            signal.signal(sig, self.replaced_signals[sig])
            del self.replaced_signals[sig]
        else:
            self.event_loop.remove_signal_handler(sig)

    def add_signal_handler(self, sig: int, handler: SignalHandler):
        """
        Add a signal handler for the given signal.

        Parameters
        ----------
        sig: int
            The signal to handle.
        handler: SignalHandler
            The handler to call when the signal is received.
        """
        if sig not in self.signal_map:
            self.signal_map[sig] = set()
            self._maybe_add_signal_handler(sig)

        self.signal_map[sig].add(handler)

    def remove_signal_handler(self, sig: signal.Signals, handler: SignalHandler):
        """
        Remove a signal handler for the given signal.

        Parameters
        ----------
        sig: int
            The signal to handle.
        handler: SignalHandler
            The handler to remove.

        Raises
        ------
        KeyError
            If the signal `sig` is not being handled.
        """
        if sig not in self.signal_map:
            return

        self.signal_map[sig].discard(handler)

    def trigger_signal(self, sig: int, frame=None):
        """
        Trigger a signal handler for the given signal.

        Parameters
        ----------
        sig : int
            The signal to handle.
        frame : [] (optional)
            The frame to pass to the signal handler.
            Only used in a synchronous context.
        """
        self._handle_signal(sig, frame)
