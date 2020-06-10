# Copyright (c) 2020 ING Wholesale Banking Advanced Analytics
# This file is part of the Population Shift Monitoring package (popmon)
# Licensed under the MIT License

import htmlmin

from ..base import Module
from ..resources import templates_env
from ..version import name, version


class ReportGenerator(Module):
    """This module takes already prepared section data, renders HTML section template with the data and
    glues sections together into one compressed report which is created based on the provided template.
    """

    def __init__(self, read_key, store_key):
        """Initialize an instance of ReportGenerator.

        :param str read_key: key of input sections data to read from the datastore
        :param str store_key: key for storing the html report code in the datastore
        """
        super().__init__()
        self.read_key = read_key
        self.store_key = store_key

    def transform(self, datastore):
        sections = self.get_datastore_object(datastore, self.read_key, dtype=list)

        # concatenate HTML sections' code
        sections_html = ""
        for i, section_info in enumerate(sections):
            sections_html += templates_env(
                filename="section.html", section_index=i, **section_info
            )

        # get HTML template for the final report, insert placeholder data and compress the code
        args = dict(sections=sections_html)
        datastore[self.store_key] = htmlmin.minify(
            templates_env(filename="core.html", generator=f"{name} {version}", **args)
        )
        return datastore
