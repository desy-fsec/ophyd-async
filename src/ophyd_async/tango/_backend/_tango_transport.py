import asyncio
import time
import functools

import numpy as np
from asyncio import CancelledError

from abc import abstractmethod
from enum import Enum
from typing import Dict, Optional, Type, Union

from tango import (AttributeInfoEx, AttrDataFormat,
                   CmdArgType, EventType,
                   GreenMode, DevState,
                   CommandInfo, AttrWriteType)

from tango.asyncio import DeviceProxy
from tango.asyncio_executor import get_global_executor, set_global_executor, AsyncioExecutor
from tango.utils import is_int, is_float, is_bool, is_str, is_binary, is_array

from bluesky.protocols import Descriptor, Reading

from ophyd_async.core import (
    NotConnected,
    ReadingValueCallback,
    SignalBackend,
    T,
    get_dtype,
    get_unique,
    wait_for_connection,
)

__all__ = ("TangoTransport", "TangoSignalBackend")


# time constant to wait for timeout
A_BIT = 1e-5

# --------------------------------------------------------------------

def ensure_proper_executor(func):
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        current_executor = get_global_executor()
        if not current_executor.in_executor_context():
            set_global_executor(AsyncioExecutor())
        return await func(self, *args, **kwargs)

    return wrapper


# --------------------------------------------------------------------
class TangoSignalBackend(SignalBackend[T]):
    # --------------------------------------------------------------------
    @abstractmethod
    async def get_w_value(self) -> T:
        """The last written value"""

    # --------------------------------------------------------------------
    @abstractmethod
    def get_signal_auto(self) -> str:
        """return signal type, passing to tango attribute/command"""


# --------------------------------------------------------------------
def get_pyton_type(tango_type) -> tuple[bool, T, str]:
    array = is_array(tango_type)
    if is_int(tango_type, True):
        return array, int, "integer"
    if is_float(tango_type, True):
        return array, float, "number"
    if is_bool(tango_type, True):
        return array, bool, "integer"
    if is_str(tango_type, True):
        return array, str, "string"
    if is_binary(tango_type, True):
        return array, list[str], "string"
    if tango_type == CmdArgType.DevEnum:
        return array, Enum, "string"
    if tango_type == CmdArgType.DevState:
        return array, CmdArgType.DevState, "string"
    if tango_type == CmdArgType.DevUChar:
        return array, int, "integer"
    if tango_type == CmdArgType.DevVoid:
        return array, None, "string"
    raise TypeError("Unknown TangoType")


# --------------------------------------------------------------------
class TangoProxy:

    support_events = False

    def __init__(self, device_proxy: DeviceProxy, name: str):
        self._proxy = device_proxy
        self._name = name

    # --------------------------------------------------------------------
    @abstractmethod
    async def get(self) -> T:
        """Get value from TRL"""

    # --------------------------------------------------------------------
    @abstractmethod
    async def get_w_value(self) -> T:
        """Get last written value from TRL"""

    # --------------------------------------------------------------------
    @abstractmethod
    async def put(self, value: Optional[T], wait: bool=True, timeout: Optional[float]=None) -> None:
        """Put value to TRL"""

    # --------------------------------------------------------------------
    @abstractmethod
    async def get_config(self) -> Union[AttributeInfoEx, CommandInfo]:
        """Get TRL config async"""

    # --------------------------------------------------------------------
    @abstractmethod
    async def get_reading(self) -> Reading:
        """Get reading from TRL"""

    # --------------------------------------------------------------------
    def has_subscription(self) -> bool:
        """indicates, that this trl already subscribed"""

    # --------------------------------------------------------------------
    @abstractmethod
    def subscribe_callback(self, callback: Optional[ReadingValueCallback]):
        """subscribe tango CHANGE event to callback"""

    # --------------------------------------------------------------------
    @abstractmethod
    def unsubscribe_callback(self):
        """delete CHANGE event subscription"""


# --------------------------------------------------------------------
class AttributeProxy(TangoProxy):

    support_events = True
    _event_callback = None
    _eid = None

    # --------------------------------------------------------------------
    @ensure_proper_executor
    async def get(self) -> T:
        attr = await self._proxy.read_attribute(self._name)
        return attr.value

    # --------------------------------------------------------------------
    @ensure_proper_executor
    async def get_w_value(self) -> T:
        attr = await self._proxy.read_attribute(self._name)
        return attr.w_value

    # --------------------------------------------------------------------
    @ensure_proper_executor
    async def put(self, value: Optional[T], wait: bool = True, timeout: Optional[float] = None) -> None:
        if wait:
            await self._proxy.write_attribute(self._name, value)
        else:
            rid = await self._proxy.write_attribute_asynch(self._name, value)
            if timeout:
                finished = False
                while not finished:
                    try:
                        val = await dev.write_attribute_reply(rid)
                        finished = True
                    except:
                        await asyncio.sleep(A_BIT)


    # --------------------------------------------------------------------
    @ensure_proper_executor
    async def get_config(self) -> AttributeInfoEx:
        return await self._proxy.get_attribute_config(self._name)

    # --------------------------------------------------------------------
    @ensure_proper_executor
    async def get_reading(self) -> Reading:
        attr = await self._proxy.read_attribute(self._name)
        return dict(
            value=attr.value,
            timestamp=attr.time.totime(),
            alarm_severity=attr.quality,
        )

    # --------------------------------------------------------------------
    def has_subscription(self) -> bool:
        return bool(self._eid)

    # --------------------------------------------------------------------
    def subscribe_callback(self, callback: Optional[ReadingValueCallback]):
        """add user callback to CHANGE event subscription"""
        self._event_callback = callback
        if not self._eid:
            self._eid = self._proxy.subscribe_event(self._name, EventType.CHANGE_EVENT, self._event_processor,
                                                    green_mode=False)

    # --------------------------------------------------------------------
    def unsubscribe_callback(self):
        if self._eid:
            self._proxy.unsubscribe_event(self._eid, green_mode=False)
            self._eid = None
        self._event_callback = None

    # --------------------------------------------------------------------
    def _event_processor(self, event):
        if not event.err:
            value = event.attr_value.value
            reading = dict(value=value,
                           timestamp=event.get_date().totime(),
                           alarm_severity=event.attr_value.quality)

            self._event_callback(reading, value)


# --------------------------------------------------------------------
class CommandProxy(TangoProxy):

    support_events = False
    _last_reading = dict(value=None, timestamp=0, alarm_severity=0)

    # --------------------------------------------------------------------
    async def get(self) -> T:
        return self._last_reading["value"]

    # --------------------------------------------------------------------
    # @ensure_proper_executor
    async def put(self, value: Optional[T], wait: bool = True, timeout: Optional[float] = None) -> None:
        if wait:
            val = await self._proxy.command_inout(self._name, value)
        else:
            val = None
            rid = await self._proxy.command_inout_asynch(self._name, value)
            if timeout:
                finished = False
                while not finished:
                    try:
                        val = await dev.command_inout_reply(rid)
                        finished = True
                    except:
                        await asyncio.sleep(A_BIT)

        self._last_reading = dict(value=val, timestamp=time.time(), alarm_severity=0)

    # --------------------------------------------------------------------
    # @ensure_proper_executor
    async def get_config(self) -> CommandInfo:
        return await self._proxy.get_command_config(self._name)

    # --------------------------------------------------------------------
    async def get_reading(self) -> Reading:
        return self._last_reading


# --------------------------------------------------------------------
def get_dtype_extended(datatype):
    # DevState tango type does not have numpy equivalents
    dtype = get_dtype(datatype)
    if dtype == np.object_:
        print(f"{datatype.__args__[1].__args__[0]=}, {datatype.__args__[1].__args__[0]==Enum}")
        if datatype.__args__[1].__args__[0] == DevState:
            dtype = CmdArgType.DevState
    return dtype


# --------------------------------------------------------------------
def get_trl_descriptor(datatype: Optional[Type], tango_resource: str,
                       tr_configs: Dict[str, Union[AttributeInfoEx, CommandInfo]]) -> dict:
    tr_dtype = {}
    for tr_name, config in tr_configs.items():
        if isinstance(config, AttributeInfoEx):
            _, dtype, descr = get_pyton_type(config.data_type)
            tr_dtype[tr_name] = config.data_format, dtype, descr
        elif isinstance(config, CommandInfo):
            if config.in_type != CmdArgType.DevVoid and \
                    config.out_type != CmdArgType.DevVoid and \
                    config.in_type != config.out_type:
                raise RuntimeError("Commands with different in and out dtypes are not supported")
            array, dtype, descr = get_pyton_type(config.in_type if config.in_type != CmdArgType.DevVoid else config.out_type)
            tr_dtype[tr_name] = AttrDataFormat.SPECTRUM \
                if array else AttrDataFormat.SCALAR, dtype, descr
        else:
            raise RuntimeError(f"Unknown config type: {type(config)}")
    tr_format, tr_dtype, tr_dtype_desc = get_unique(tr_dtype, "typeids")

    # tango commands are limited in functionality: they do not have info about shape and Enum labels
    trl_config = list(tr_configs.values())[0]
    max_x = trl_config.max_dim_x if hasattr(trl_config, "max_dim_x") else np.Inf
    max_y = trl_config.max_dim_y if hasattr(trl_config, "max_dim_y") else np.Inf
    is_attr = hasattr(trl_config, "enum_labels")
    trl_choices = list(trl_config.enum_labels) if is_attr else []

    if tr_format in [AttrDataFormat.SPECTRUM, AttrDataFormat.IMAGE]:
        # This is an array
        if datatype:
            # Check we wanted an array of this type
            dtype = get_dtype_extended(datatype)
            if not dtype:
                raise TypeError(f"{tango_resource} has type [{tr_dtype}] not {datatype.__name__}")
            if dtype != tr_dtype:
                raise TypeError(f"{tango_resource} has type [{tr_dtype}] not [{dtype}]")

        if tr_format == AttrDataFormat.SPECTRUM:
            return dict(source=tango_resource, dtype="array", shape=[max_x])
        elif tr_format == AttrDataFormat.IMAGE:
            return dict(source=tango_resource, dtype="array", shape=[max_y, max_x])

    else:
        if tr_dtype in (Enum, CmdArgType.DevState):
            if tr_dtype == CmdArgType.DevState:
                trl_choices = list(DevState.names.keys())

            if datatype:
                if not issubclass(datatype, (Enum, DevState)):
                    raise TypeError(f"{tango_resource} has type Enum not {datatype.__name__}")
                if tr_dtype == Enum and is_attr:
                    choices = tuple(v.name for v in datatype)
                    if set(choices) != set(trl_choices):
                        raise TypeError(f"{tango_resource} has choices {trl_choices} not {choices}")
            return dict(source=tango_resource, dtype="string", shape=[], choices=trl_choices)
        else:
            if datatype and not issubclass(tr_dtype, datatype):
                raise TypeError(f"{tango_resource} has type {tr_dtype.__name__} not {datatype.__name__}")
            return dict(source=tango_resource, dtype=tr_dtype_desc, shape=[])


# --------------------------------------------------------------------
async def get_tango_trl(full_trl: str, device_proxy: Optional[DeviceProxy]) -> TangoProxy:
    device_trl, trl_name = full_trl.rsplit('/', 1)
    device_proxy = device_proxy or await DeviceProxy(device_trl)
    if trl_name in device_proxy.get_attribute_list():
        return AttributeProxy(device_proxy, trl_name)
    if trl_name in device_proxy.get_command_list():
        return CommandProxy(device_proxy, trl_name)
    if trl_name in device_proxy.get_pipe_list():
        raise NotImplemented("Pipes are not supported")

    raise RuntimeError(f"{trl_name} cannot be found in {device_proxy.name()}")


# --------------------------------------------------------------------
class TangoTransport(TangoSignalBackend[T]):

    def __init__(self,
                 datatype: Optional[Type[T]],
                 read_trl: str,
                 write_trl: str,
                 device_proxy: Optional[DeviceProxy] = None):
        self.datatype = datatype
        self.read_trl = read_trl
        self.write_trl = write_trl
        self.proxies: Dict[str, TangoProxy] = {read_trl: device_proxy, write_trl: device_proxy}
        self.trl_configs: Dict[str, AttributeInfoEx] = {}
        self.source = f"{self.read_trl}"
        self.descriptor: Descriptor = {}  # type: ignore

    # --------------------------------------------------------------------
    async def _connect_and_store_config(self, trl):
        try:
            self.proxies[trl] = await get_tango_trl(trl, self.proxies[trl])
            self.trl_configs[trl] = await self.proxies[trl].get_config()
        except CancelledError:
            raise NotConnected(self.source)

    # --------------------------------------------------------------------
    async def connect(self):
        if self.read_trl != self.write_trl:
            # Different, need to connect both
            await wait_for_connection(
                read_trl=self._connect_and_store_config(self.read_trl),
                write_trl=self._connect_and_store_config(self.write_trl),
            )
        else:
            # The same, so only need to connect one
            await self._connect_and_store_config(self.read_trl)
        self.descriptor = get_trl_descriptor(self.datatype, self.read_trl, self.trl_configs)

    # --------------------------------------------------------------------
    async def put(self, write_value: Optional[T], wait=True, timeout=None):
        await self.proxies[self.write_trl].put(write_value, wait, timeout)

    # --------------------------------------------------------------------
    async def get_descriptor(self) -> Descriptor:
        return self.descriptor

    # --------------------------------------------------------------------
    async def get_reading(self) -> Reading:
        return await self.proxies[self.read_trl].get_reading()

    # --------------------------------------------------------------------
    async def get_value(self) -> T:
        return await self.proxies[self.write_trl].get()

    # --------------------------------------------------------------------
    async def get_w_value(self) -> T:
        return await self.proxies[self.write_trl].get_w_value()

    # --------------------------------------------------------------------
    def set_callback(self, callback: Optional[ReadingValueCallback]) -> None:
        assert self.proxies[self.read_trl].support_events, f"{self.source} does not support events"

        if callback:
            assert (not self.proxies[self.read_trl].has_subscription()), "Cannot set a callback when one is already set"
            try:
                self.proxies[self.read_trl].subscribe_callback(callback)
            except Exception as err:
                raise RuntimeError(f"Cannot set event for {self.read_trl}. "
                                   f"This signal should be used only as non-cached!")

        else:
            self.proxies[self.read_trl].unsubscribe_callback()