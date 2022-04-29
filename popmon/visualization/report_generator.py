# Copyright (c) 2022 ING Wholesale Banking Advanced Analytics
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


import htmlmin

from ..base import Module
from ..resources import templates_env
from ..version import version


class ReportGenerator(Module):
    """This module takes already prepared section data, renders HTML section template with the data and
    glues sections together into one compressed report which is created based on the provided template.
    """

    _input_keys = ("read_key",)
    _output_keys = ("store_key",)

    def __init__(self, read_key, store_key):
        """Initialize an instance of ReportGenerator.

        :param str read_key: key of input sections data to read from the datastore
        :param str store_key: key for storing the html report code in the datastore
        """
        super().__init__()
        self.read_key = read_key
        self.store_key = store_key

    def get_description(self):
        return "HTML Report"

    def transform(self, sections: list) -> str:
        # concatenate HTML sections' code
        sections_html = ""
        for i, section_info in enumerate(sections):
            sections_html += templates_env(
                filename="section.html", section_index=i, **section_info
            )

        # get HTML template for the final report, insert placeholder data and compress the code
        return htmlmin.minify(
            templates_env(
                filename="core.html",
                generator=f"popmon {version}",
                sections=sections_html,
            )
        )
