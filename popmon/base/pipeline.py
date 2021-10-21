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

import json
import logging
from pathlib import Path


class Pipeline:
    """Base class used for to run modules in a pipeline."""

    def __init__(self, modules, logger=None):
        """Initialization of the pipeline

        :param list modules: modules of the pipeline.
        :param logger: logger to be used by each module.
        """
        self.modules = modules
        self.set_logger(logger)

    def set_logger(self, logger):
        """Set the logger to be used by each module

        :param logger: input logger
        """
        self.logger = logger
        if self.logger is None:
            self.logger = logging.getLogger()
        for module in self.modules:
            module.set_logger(self.logger)

    def add_modules(self, modules):
        """Add more modules to existing list of modules.

        :param list modules: list of more modules
        """
        if len(modules):
            self.modules.extend(modules)

    def transform(self, datastore):
        """Central function of the pipeline.

        Calls transform() of each module in the pipeline.
        Typically transform() of a module takes something from the datastore, does something to it,
        and puts the results back into the datastore again, to be passed on to the next module in the pipeline.

        :param dict datastore: input datastore
        :return: updated output datastore
        :rtype: dict
        """

        for module in self.modules:
            self.logger.debug(f"transform {module.__class__.__name__}")
            if isinstance(module, Pipeline):
                datastore = module.transform(datastore)
            else:
                datastore = module._transform(datastore)
        return datastore

    def visualize(self, versioned=True, funcs=None, dsets=None):
        if dsets is None:
            dsets = {}
        if funcs is None:
            funcs = {}

        modules = []
        for module in self.modules:
            name = module.__class__.__name__
            if isinstance(module, Pipeline):
                modules.append(module.visualize(versioned, funcs, dsets))
            else:
                in_keys = module.get_inputs()

                if versioned:
                    new_ins = {}
                    for k, in_key in in_keys.items():
                        if in_key not in dsets:
                            dsets[in_key] = 1
                        in_key += f" (v{dsets[in_key]})"
                        new_ins[k] = in_key
                    in_keys = new_ins

                out_keys = module.get_outputs()
                if versioned:
                    new_outs = {}
                    for k, out_key in out_keys.items():
                        if out_key in dsets:
                            dsets[out_key] += 1
                        else:
                            dsets[out_key] = 1
                        out_key += f" (v{dsets[out_key]})"
                        new_outs[k] = out_key
                    out_keys = new_outs

                self.logger.debug(f"{name}(inputs={in_keys}, outputs={out_keys})")

                # add unique id
                if name not in funcs:
                    funcs[name] = {}
                if id(module) not in funcs[name]:
                    funcs[name][id(module)] = len(funcs[name]) + 1

                modules.append(
                    {
                        "type": "module",
                        "name": f"{name}",
                        "i": f"{funcs[name][id(module)]}",
                        "desc": module.get_description(),
                        "in": in_keys,
                        "out": out_keys,
                    }
                )
        data = {"type": "subgraph", "name": self.__class__.__name__, "modules": modules}
        return data

    def to_json(self, file_name, versioned=True):
        d = self.visualize(versioned=versioned)
        data = json.dumps(d, indent=4, sort_keys=True)
        Path(file_name).write_text(data)
