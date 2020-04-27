# flake8: noqa

# set matplotlib backend to batchmode when running in shell
# need to do this *before* matplotlib.pyplot gets imported
from..visualization.backend import set_matplotlib_backend
set_matplotlib_backend()

from popmon.visualization.section_generator import SectionGenerator
from popmon.visualization.histogram_section import HistogramSection
from popmon.visualization.report_generator import ReportGenerator

__all__ = ["SectionGenerator", "HistogramSection", "ReportGenerator"]
