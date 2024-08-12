import asyncio
from enum import Enum

import numpy as np
import numpy.typing as npt
import pytest
from test_tango_signals import (
    make_backend,
    prepare_device,
)

from ophyd_async.tango._backend._tango_transport import (
    AttributeProxy,
    CommandProxy,
    ensure_proper_executor,
    get_dtype_extended,
    get_python_type,
    get_tango_trl,
    get_trl_descriptor,
)
from tango import (
    CmdArgType,
    DevState,
)
from tango.asyncio import DeviceProxy
from tango.asyncio_executor import (
    AsyncioExecutor,
    get_global_executor,
)


# --------------------------------------------------------------------
@pytest.fixture(scope="module")
async def device_proxy(tango_test_device):
    return await DeviceProxy(tango_test_device)


# --------------------------------------------------------------------
@pytest.fixture(scope="module")
async def transport(echo_device):
    await prepare_device(echo_device, "float_scalar_attr", 1.0)
    source = echo_device + "/" + "float_scalar_attr"
    return await make_backend(float, source, connect=True)


# --------------------------------------------------------------------
class HelperClass:
    @ensure_proper_executor
    async def mock_func(self):
        return "executed"


# Test function
@pytest.mark.asyncio
async def test_ensure_proper_executor():
    # Instantiate the helper class and call the decorated method
    helper_instance = HelperClass()
    result = await helper_instance.mock_func()

    # Assertions
    assert result == "executed"
    assert isinstance(get_global_executor(), AsyncioExecutor)


# --------------------------------------------------------------------
@pytest.mark.parametrize(
    "tango_type, expected",
    [
        (CmdArgType.DevVoid, (False, None, "string")),
        (CmdArgType.DevBoolean, (False, bool, "integer")),
        (CmdArgType.DevShort, (False, int, "integer")),
        (CmdArgType.DevLong, (False, int, "integer")),
        (CmdArgType.DevFloat, (False, float, "number")),
        (CmdArgType.DevDouble, (False, float, "number")),
        (CmdArgType.DevUShort, (False, int, "integer")),
        (CmdArgType.DevULong, (False, int, "integer")),
        (CmdArgType.DevString, (False, str, "string")),
        (CmdArgType.DevVarCharArray, (True, list[str], "string")),
        (CmdArgType.DevVarShortArray, (True, int, "integer")),
        (CmdArgType.DevVarLongArray, (True, int, "integer")),
        (CmdArgType.DevVarFloatArray, (True, float, "number")),
        (CmdArgType.DevVarDoubleArray, (True, float, "number")),
        (CmdArgType.DevVarUShortArray, (True, int, "integer")),
        (CmdArgType.DevVarULongArray, (True, int, "integer")),
        (CmdArgType.DevVarStringArray, (True, str, "string")),
        # (CmdArgType.DevVarLongStringArray, (True, str, "string")),
        # (CmdArgType.DevVarDoubleStringArray, (True, str, "string")),
        (CmdArgType.DevState, (False, CmdArgType.DevState, "string")),
        (CmdArgType.ConstDevString, (False, str, "string")),
        (CmdArgType.DevVarBooleanArray, (True, bool, "integer")),
        (CmdArgType.DevUChar, (False, int, "integer")),
        (CmdArgType.DevLong64, (False, int, "integer")),
        (CmdArgType.DevULong64, (False, int, "integer")),
        (CmdArgType.DevVarLong64Array, (True, int, "integer")),
        (CmdArgType.DevVarULong64Array, (True, int, "integer")),
        (CmdArgType.DevEncoded, (False, list[str], "string")),
        (CmdArgType.DevEnum, (False, Enum, "string")),
        # (CmdArgType.DevPipeBlob, (False, list[str], "string")),
    ],
)
def test_get_python_type(tango_type, expected):
    assert get_python_type(tango_type) == expected


# --------------------------------------------------------------------
@pytest.mark.parametrize(
    "datatype, expected",
    [
        (npt.NDArray[np.float64], np.dtype("float64")),
        (npt.NDArray[np.int8], np.dtype("int8")),
        (npt.NDArray[np.uint8], np.dtype("uint8")),
        (npt.NDArray[np.int32], np.dtype("int32")),
        (npt.NDArray[np.int64], np.dtype("int64")),
        (npt.NDArray[np.uint16], np.dtype("uint16")),
        (npt.NDArray[np.uint32], np.dtype("uint32")),
        (npt.NDArray[np.uint64], np.dtype("uint64")),
        (npt.NDArray[np.bool_], np.dtype("bool")),
        (npt.NDArray[DevState], CmdArgType.DevState),
        (npt.NDArray[np.str_], np.dtype("str")),
        (npt.NDArray[np.float32], np.dtype("float32")),
        (npt.NDArray[np.complex64], np.dtype("complex64")),
        (npt.NDArray[np.complex128], np.dtype("complex128")),
    ],
)
def test_get_dtype_extended(datatype, expected):
    assert get_dtype_extended(datatype) == expected


# --------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "datatype, tango_resource, expected_descriptor",
    [
        (
            int,
            "test/device/1/justvalue",
            {"source": "test/device/1/justvalue", "dtype": "integer", "shape": []},
        ),
        (
            float,
            "test/device/1/limitedvalue",
            {"source": "test/device/1/limitedvalue", "dtype": "number", "shape": []},
        ),
        (
            npt.NDArray[float],
            "test/device/1/array",
            {"source": "test/device/1/array", "dtype": "array", "shape": [2, 3]},
        ),
        # Add more test cases as needed
    ],
)
async def test_get_trl_descriptor(
    tango_test_device, datatype, tango_resource, expected_descriptor
):
    proxy = await DeviceProxy(tango_test_device)
    tr_configs = {
        tango_resource.split("/")[-1]: await proxy.get_attribute_config(
            tango_resource.split("/")[-1]
        )
    }
    descriptor = get_trl_descriptor(datatype, tango_resource, tr_configs)
    assert descriptor == expected_descriptor


# --------------------------------------------------------------------
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "trl, proxy_needed, expected_type, should_raise",
    [
        ("test/device/1/justvalue", True, AttributeProxy, False),
        ("test/device/1/justvalue", False, AttributeProxy, False),
        ("test/device/1/clear", True, CommandProxy, False),
        ("test/device/1/clear", False, CommandProxy, False),
        ("test/device/1/nonexistent", True, None, True),
    ],
)
async def test_get_tango_trl(
    tango_test_device, trl, proxy_needed, expected_type, should_raise
):
    proxy = await DeviceProxy(tango_test_device) if proxy_needed else None
    if should_raise:
        with pytest.raises(RuntimeError):
            await get_tango_trl(trl, proxy)
    else:
        result = await get_tango_trl(trl, proxy)
        assert isinstance(result, expected_type)


# --------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("attr", ["justvalue", "array"])
async def test_attribute_proxy_get(device_proxy, attr):
    attr_proxy = AttributeProxy(device_proxy, attr)
    val = None
    val = await attr_proxy.get()
    assert val is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("attr", ["justvalue", "array"])
async def test_attribute_proxy_put(device_proxy, attr):
    attr_proxy = AttributeProxy(device_proxy, attr)
    old_value = await attr_proxy.get()
    new_value = old_value + 1
    await attr_proxy.put(new_value, wait=True)
    updated_value = await attr_proxy.get()
    if isinstance(new_value, np.ndarray):
        assert np.all(updated_value == new_value)
    else:
        assert updated_value == new_value


# --------------------------------------------------------------------
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "attr, new_value", [("justvalue", 10), ("array", np.array([[2, 3, 4], [5, 6, 7]]))]
)
async def test_attribute_proxy_get_w_value(device_proxy, attr, new_value):
    attr_proxy = AttributeProxy(device_proxy, attr)
    await attr_proxy.put(new_value)
    attr_proxy_value = await attr_proxy.get()
    if isinstance(new_value, np.ndarray):
        assert np.all(attr_proxy_value == new_value)
    else:
        assert attr_proxy_value == new_value


# --------------------------------------------------------------------
@pytest.mark.asyncio
async def test_attribute_get_config(device_proxy):
    attr_proxy = AttributeProxy(device_proxy, "justvalue")
    config = await attr_proxy.get_config()
    assert config.writable is not None


# --------------------------------------------------------------------
@pytest.mark.asyncio
async def test_attribute_get_reading(device_proxy):
    attr_proxy = AttributeProxy(device_proxy, "justvalue")
    reading = await attr_proxy.get_reading()
    assert reading["value"] is not None


# --------------------------------------------------------------------
def test_attribute_has_subscription(device_proxy):
    attr_proxy = AttributeProxy(device_proxy, "justvalue")
    expected = bool(attr_proxy._callback)
    has_subscription = attr_proxy.has_subscription()
    assert has_subscription is expected


# --------------------------------------------------------------------
@pytest.mark.asyncio
async def test_attribute_subscribe_callback(echo_device):
    await prepare_device(echo_device, "float_scalar_attr", 1.0)
    source = echo_device + "/" + "float_scalar_attr"
    backend = await make_backend(float, source)
    attr_proxy = backend.proxies[source]
    val = None

    def callback(reading, value):
        print("Callback called")
        nonlocal val
        val = value

    attr_proxy.subscribe_callback(callback)
    assert attr_proxy.has_subscription()
    old_value = await attr_proxy.get()
    new_value = old_value + 1
    await attr_proxy.put(new_value)
    await asyncio.sleep(0.2)
    attr_proxy.unsubscribe_callback()
    assert val == new_value


# --------------------------------------------------------------------
@pytest.mark.asyncio
async def test_attribute_unsubscribe_callback(echo_device):
    await prepare_device(echo_device, "float_scalar_attr", 1.0)
    source = echo_device + "/" + "float_scalar_attr"
    backend = await make_backend(float, source)
    attr_proxy = backend.proxies[source]

    def callback(reading, value):
        pass

    attr_proxy.subscribe_callback(callback)
    assert attr_proxy.has_subscription()
    attr_proxy.unsubscribe_callback()
    assert not attr_proxy.has_subscription()


# --------------------------------------------------------------------
def test_attribute_set_polling(device_proxy):
    attr_proxy = AttributeProxy(device_proxy, "justvalue")
    attr_proxy.set_polling(True, 0.1, 1, 0.1)
    assert attr_proxy._allow_polling
    assert attr_proxy._polling_period == 0.1
    assert attr_proxy._abs_change == 1
    assert attr_proxy._rel_change == 0.1
    attr_proxy.set_polling(False)


# --------------------------------------------------------------------
@pytest.mark.asyncio
async def test_attribute_poll(device_proxy):
    attr_proxy = AttributeProxy(device_proxy, "justvalue")
    attr_proxy.set_polling(True, 0.1, 1)
    val = None

    def callback(reading, value):
        nonlocal val
        val = value

    attr_proxy.subscribe_callback(callback)
    current_value = await attr_proxy.get()
    new_value = current_value + 2
    await attr_proxy.put(new_value)
    polling_period = attr_proxy._polling_period
    await asyncio.sleep(polling_period)
    attr_proxy.set_polling(False)
    assert val is not None


# --------------------------------------------------------------------
@pytest.mark.asyncio
async def test_command_put(device_proxy):
    cmd_proxy = CommandProxy(device_proxy, "clear")
    await cmd_proxy.put(None, wait=True, timeout=1.0)
    assert cmd_proxy._last_reading["value"] is not None


# --------------------------------------------------------------------
@pytest.mark.asyncio
async def test_command_get(device_proxy):
    cmd_proxy = CommandProxy(device_proxy, "clear")
    await cmd_proxy.put(None, wait=True, timeout=1.0)
    reading = cmd_proxy._last_reading
    assert reading["value"] is not None


# --------------------------------------------------------------------
@pytest.mark.asyncio
async def test_command_get_config(device_proxy):
    cmd_proxy = CommandProxy(device_proxy, "clear")
    config = await cmd_proxy.get_config()
    assert config.out_type is not None


# --------------------------------------------------------------------
@pytest.mark.asyncio
async def test_command_get_reading(device_proxy):
    cmd_proxy = CommandProxy(device_proxy, "clear")
    await cmd_proxy.put(None, wait=True, timeout=1.0)
    reading = await cmd_proxy.get_reading()
    assert reading["value"] is not None


# --------------------------------------------------------------------
def test_command_set_polling(device_proxy):
    cmd_proxy = CommandProxy(device_proxy, "clear")
    cmd_proxy.set_polling(True, 0.1)
    # Set polling in the command proxy currently does nothing
    assert True


# --------------------------------------------------------------------
@pytest.mark.asyncio
async def test_tango_transport_init(echo_device):
    await prepare_device(echo_device, "float_scalar_attr", 1.0)
    source = echo_device + "/" + "float_scalar_attr"
    transport = await make_backend(float, source, connect=False)
    assert transport is not None


# --------------------------------------------------------------------
@pytest.mark.asyncio
async def test_tango_transport_source(echo_device):
    await prepare_device(echo_device, "float_scalar_attr", 1.0)
    source = echo_device + "/" + "float_scalar_attr"
    transport = await make_backend(float, source)
    transport_source = transport.source("")
    assert transport_source == source


# --------------------------------------------------------------------
@pytest.mark.asyncio
async def test_tango_transport_connect(echo_device):
    await prepare_device(echo_device, "float_scalar_attr", 1.0)
    source = echo_device + "/" + "float_scalar_attr"
    backend = await make_backend(float, source, connect=True)
    assert backend is not None


# --------------------------------------------------------------------
@pytest.mark.asyncio
async def test_tango_transport_connect_and_store_config(echo_device):
    await prepare_device(echo_device, "float_scalar_attr", 1.0)
    source = echo_device + "/" + "float_scalar_attr"
    transport = await make_backend(float, source, connect=False)
    await transport._connect_and_store_config(source)
    assert transport.trl_configs[source] is not None


# --------------------------------------------------------------------
@pytest.mark.asyncio
async def test_tango_transport_put(transport):
    source = transport.source("")
    await transport.put(2.0)
    val = await transport.proxies[source].get_w_value()
    assert val == 2.0


# --------------------------------------------------------------------
@pytest.mark.asyncio
async def test_tango_transport_get_datakey(transport):
    source = transport.source("")
    datakey = await transport.get_datakey(source)
    assert datakey["source"] == source
    assert datakey["dtype"] == "number"
    assert datakey["shape"] == []


# --------------------------------------------------------------------
@pytest.mark.asyncio
async def test_tango_transport_get_reading(transport):
    reading = await transport.get_reading()
    assert reading["value"] == 1.0


# --------------------------------------------------------------------
@pytest.mark.asyncio
async def test_tango_transport_get_value(transport):
    value = await transport.get_value()
    assert value == 1.0


# --------------------------------------------------------------------
@pytest.mark.asyncio
async def test_tango_transport_get_setpoint(transport):
    new_setpoint = 2.0
    await transport.put(new_setpoint)
    setpoint = await transport.get_setpoint()
    assert setpoint == new_setpoint


# --------------------------------------------------------------------
@pytest.mark.asyncio
async def test_set_callback(transport):
    val = None

    def callback(reading, value):
        nonlocal val
        val = value

    transport.set_callback(callback)
    current_value = await transport.get_value()
    new_value = current_value + 2
    await transport.put(new_value)
    await asyncio.sleep(0.1)
    transport.set_callback(None)
    assert val == new_value


# --------------------------------------------------------------------
@pytest.mark.asyncio
async def test_tango_transport_set_polling(transport):
    source = transport.source("")
    transport.set_polling(True, 0.1, 1, 0.1)
    assert transport.polling == (True, 0.1, 1, 0.1)
    assert transport.proxies[source]._allow_polling
    assert transport.proxies[source]._polling_period == 0.1
    assert transport.proxies[source]._abs_change == 1
    assert transport.proxies[source]._rel_change == 0.1


# --------------------------------------------------------------------
@pytest.mark.asyncio
@pytest.mark.parametrize("allow", [True, False])
async def test_tango_transport_allow_events(transport, allow):
    transport.allow_events(allow)
    assert transport.support_events == allow