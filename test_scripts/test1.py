#run with ipython -i
import asyncio

from tango import AttrQuality, AttrDataFormat, AttrWriteType, DeviceProxy, DevState, CmdArgType
from tango.server import Device, attribute, command
from tango.test_context import MultiDeviceTestContext

from ophyd_async.core import DeviceCollector
from ophyd_async.tango import TangoReadableDevice, tango_signal_r

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
    def just_a_value(self):
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
        yield context.get_device_access("test/device/1")


### ophyd device

class TestReadableDevice(TangoReadableDevice):

    def __init__(self, trl: str, name="") -> None:
        TangoReadableDevice.__init__(self, trl, name)
        self._set_success = True

    def register_signals(self):

        self.just_a_value = tango_signal_r(int, self.trl + '/just_a_value', device_proxy=self.proxy)

        self.set_readable_signals(read_uncached=[self.just_a_value])
#                                        config=[self.baserate,
#                                                self.velocity,
#                                                self.acceleration,
#                                                self.deceleration])
        
        self._state = tango_signal_r(DevState, self.trl + '/State', self.proxy)


### ....... below a simple dem script, trying to use a tango ophyd device in bluesky plan ... 
### ... best to run with ipython -i



async def main():
    for tango_dev in tango_test_device():

        # --------------------------------------------------------------------

        async with DeviceCollector():
            ophyd_dev = await TestReadableDevice(tango_dev)

        print(ophyd_dev.just_a_value)

        #now lets do some bluesky stuff
        RE = RunEngine()
        print("####mv ",RE(bps.rd(ophyd_dev)))



if __name__ == "__main__":
    asyncio.run(main())