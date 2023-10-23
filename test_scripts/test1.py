#run with ipython -i

from tango import AttrQuality, AttrDataFormat, AttrWriteType, DeviceProxy, DevState, CmdArgType
from tango.server import Device, attribute, command
from tango.test_context import MultiDeviceTestContext

from ophyd_async.tango.device import TangoStandardReadableDevice, ReadableSignal

import numpy as np
import numpy.typing as npt

from bluesky import RunEngine
import bluesky.plan_stubs as bps

class TestDevice(Device):
    __test__ = False

    _array = [[1, 2, 3],
              [4, 5, 6]]

    _limitedvalue = 3

    @attribute(dtype=int)
    def justvalue(self):
        return 5

    @attribute(dtype=float, access=AttrWriteType.READ_WRITE,
               dformat=AttrDataFormat.IMAGE, max_dim_x=3, max_dim_y=2)
    def array(self) -> list[list[float]]:
        return self._array

    def write_array(self, array: list[list[float]]):
        self._array = array

    @attribute(dtype=float, access=AttrWriteType.READ_WRITE,
               min_value=0, min_alarm=1, min_warning=2,
               max_warning=4, max_alarm=5, max_value=6)
    def limitedvalue(self) -> float:
        return self._limitedvalue

    def write_limitedvalue(self, value: float):
        self._limitedvalue = value

def tango_test_device():
    with MultiDeviceTestContext(
            [{"class": TestDevice, "devices": [{"name": "test/device/1"}]}], process=True) as context:
        yield context.get_device("test/device/1")



### ....... below a simple dem script, trying to use a tango ophyd device in bluesky plan ... 
### ... best to run with ipython -i


for tango_dev in tango_test_device():
    print(tango_dev.justvalue)

    #TODO: transfor mit ophyd-tango
    class TestReadableDevice(TangoStandardReadableDevice):
        justvalue: ReadableSignal[int]
        array: ReadableSignal[npt.NDArray[float]]
        limitedvalue: ReadableSignal[float]

    ophyd_dev= TestReadableDevice(tango_dev)

    #now lets do some bluesky stuff
    RE = RunEngine()
    print("####mv ",RE(bps.rd(ophyd_dev)))