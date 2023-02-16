# Copyright (c) 2023 ING Analytics Wholesale Banking
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
from datetime import datetime

from popmon.base import Module


class Timing(Module):
    """Module to add the current timestamp to the datastore."""

    _input_keys = ()
    _output_keys = ("store_key",)

    def __init__(
        self,
        store_key: str,
        **kwargs,
    ) -> None:
        """Initialize an instance.

        :param str store_key: key of input data to be stored in the datastore
        """
        super().__init__()

        self.store_key = store_key
        self.kwargs = kwargs

    def transform(self):
        data = datetime.now()
        self.logger.info(f"storing time to {self.store_key}")
        return data
