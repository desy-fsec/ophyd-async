from __future__ import annotations

from typing import Callable, Optional

from ophyd_async.core import SignalR, SignalRW, SignalW, SignalX, T
from ophyd_async.core.signal import add_timeout


# --------------------------------------------------------------------
# from tango attributes one can get setvalue, so we extend SignalRW and SignalW
class SignalWithSetpoint:
    @add_timeout
    async def get_setpoint(self, cached: Optional[bool] = None) -> T:
        """The last written value to TRL"""
        return await self._backend_or_cache(cached).get_w_value()


# --------------------------------------------------------------------
# not every tango attribute is configured to generate signals
class CachableOrNot:
    def is_cachable(self) -> T:
        """The last written value to TRL"""
        return self._backend.is_cachable()


# --------------------------------------------------------------------
class TangoSignalW(SignalW[T], CachableOrNot, SignalWithSetpoint):
    # --------------------------------------------------------------------
    @property
    def source(self) -> Callable[[], str]:
        return self._backend.source


# --------------------------------------------------------------------
class TangoSignalRW(SignalRW[T], CachableOrNot, SignalWithSetpoint):
    # --------------------------------------------------------------------
    @property
    def source(self) -> Callable[[], str]:
        return self._backend.source


# --------------------------------------------------------------------
class TangoSignalR(SignalR[T], CachableOrNot):
    # --------------------------------------------------------------------
    @property
    def source(self) -> Callable[[], str]:
        return self._backend.source


# --------------------------------------------------------------------
class TangoSignalX(SignalX, CachableOrNot):
    # --------------------------------------------------------------------
    @property
    def source(self) -> Callable[[], str]:
        return self._backend.source