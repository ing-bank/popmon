# Copyright (c) 2020 ING Wholesale Banking Advanced Analytics
# This file is part of the Population Shift Monitoring package (popmon)
# Licensed under the MIT License

# flake8: noqa

from popmon.visualization.histogram_section import HistogramSection
from popmon.visualization.report_generator import ReportGenerator
from popmon.visualization.section_generator import SectionGenerator

# set matplotlib backend to batchmode when running in shell
# need to do this *before* matplotlib.pyplot gets imported
from ..visualization.backend import set_matplotlib_backend

set_matplotlib_backend()


__all__ = ["SectionGenerator", "HistogramSection", "ReportGenerator"]
