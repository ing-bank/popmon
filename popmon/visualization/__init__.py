# Copyright (c) 2021 ING Wholesale Banking Advanced Analytics
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


# flake8: noqa

from popmon.visualization.alert_section_generator import AlertSectionGenerator
from popmon.visualization.histogram_section import HistogramSection
from popmon.visualization.report_generator import ReportGenerator
from popmon.visualization.section_generator import SectionGenerator
from popmon.visualization.traffic_light_section_generator import (
    TrafficLightSectionGenerator,
)

# set matplotlib backend to batch mode when running in shell
# need to do this *before* matplotlib.pyplot gets imported
from ..visualization.backend import set_matplotlib_backend

set_matplotlib_backend()


__all__ = [
    "SectionGenerator",
    "HistogramSection",
    "ReportGenerator",
    "TrafficLightSectionGenerator",
    "AlertSectionGenerator",
]
