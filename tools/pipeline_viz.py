import json
from itertools import cycle
from pathlib import Path

import pygraphviz as pgv

from popmon.base import Pipeline


def serialize_module(module, versioned, funcs, dsets):
    in_keys = module.get_inputs()
    name = module.__class__.__name__

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

    # add unique id
    if name not in funcs:
        funcs[name] = {}
    if id(module) not in funcs[name]:
        funcs[name][id(module)] = len(funcs[name]) + 1

    return {
        "type": "module",
        "name": f"{name}",
        "i": f"{funcs[name][id(module)]}",
        "desc": module.get_description(),
        "in": in_keys,
        "out": out_keys,
    }


def serialize_pipeline(pipeline, versioned=True, funcs=None, dsets=None):
    if dsets is None:
        dsets = {}
    if funcs is None:
        funcs = {}

    modules = []
    for module in pipeline.modules:
        if isinstance(module, Pipeline):
            modules.append(serialize_pipeline(module, versioned, funcs, dsets))
        else:
            modules.append(serialize_module(module, versioned, funcs, dsets))
    return {"type": "pipeline", "name": pipeline.__class__.__name__, "modules": modules}


def pipeline_to_json(pipeline, file_name, versioned=True):
    d = serialize_pipeline(pipeline, versioned=versioned)
    data = json.dumps(d, indent=4, sort_keys=True)
    Path(file_name).write_text(data)


def generate_pipeline_visualisation(
    input_file,
    output_file,
    include_subgraphs: bool = False,
    include_labels: bool = False,
):
    data = Path(input_file).read_text()
    data = json.loads(data)

    tableau20 = [
        (31, 119, 180),
        (174, 199, 232),
        (255, 127, 14),
        (255, 187, 120),
        (44, 160, 44),
        (152, 223, 138),
        (214, 39, 40),
        (255, 152, 150),
        (148, 103, 189),
        (197, 176, 213),
        (140, 86, 75),
        (196, 156, 148),
        (227, 119, 194),
        (247, 182, 210),
        (127, 127, 127),
        (199, 199, 199),
        (188, 189, 34),
        (219, 219, 141),
        (23, 190, 207),
        (158, 218, 229),
    ]

    colors = [f"#{r:02x}{g:02x}{b:02x}" for r, g, b in tableau20]
    pipeline_colors = cycle(colors)
    pipeline_style = {}
    module_style = {"shape": "rectangle", "fillcolor": "chartreuse", "style": "filled"}
    dataset_style = {"shape": "oval", "fillcolor": "orange", "style": "filled"}
    edge_style = {"fontcolor": "gray50"}

    def get_module_label(module):
        label = f"<{module['name']}"
        d = module.get("desc", "")
        if len(d) > 0:
            label += f" <BR/><I>{d}</I>"
        label += ">"
        return label

    def process(data, G):
        if data["type"] == "pipeline":
            if include_subgraphs:
                c = G.add_subgraph(
                    name=f'cluster_{data["name"]}',
                    label=data["name"],
                    color=next(pipeline_colors),
                    **pipeline_style,
                )
            else:
                c = G
            for m in data["modules"]:
                process(m, c)
        elif data["type"] == "module":
            name = f"{data['name']}_{data['i']}"
            G.add_node(name, label=get_module_label(data), **module_style)

            for k, v in data["in"].items():
                kwargs = {}
                if include_labels:
                    kwargs["headlabel"] = k
                G.add_edge(v, name, **edge_style, **kwargs)
            for k, v in data["out"].items():
                kwargs = {}
                if include_labels:
                    kwargs["taillabel"] = k
                G.add_edge(name, v, **edge_style, **kwargs)
        else:
            raise ValueError("type should be 'pipeline' or 'module'")

    g = pgv.AGraph(name="popmon-pipeline", directed=True)
    g.node_attr.update(**dataset_style)
    process(data, g)
    g.layout("dot")
    g.draw(output_file)


if __name__ == "__main__":
    data_path = Path(".")

    # Example pipeline
    from popmon import resources
    from popmon.config import config
    from popmon.pipeline.amazing_pipeline import AmazingPipeline

    cfg = {
        **config,
        "histograms_path": resources.data("synthetic_histograms.json"),
        "hists_key": "hists",
        "ref_hists_key": "hists",
        "datetime_name": "date",
        "window": 20,
        "shift": 1,
        "monitoring_rules": {
            "*_pull": [7, 4, -4, -7],
            "*_zscore": [7, 4, -4, -7],
        },
        "pull_rules": {"*_pull": [7, 4, -4, -7]},
        "show_stats": config["limited_stats"],
    }

    pipeline = AmazingPipeline(**cfg)
    name = pipeline.__class__.__name__.lower()

    input_file = data_path / f"pipeline_{name}_unversioned.json"
    pipeline_to_json(pipeline, input_file, versioned=False)
    output_file = f"pipeline_{name}_subgraphs_unversioned.pdf"
    generate_pipeline_visualisation(input_file, output_file, include_subgraphs=True)
    output_file = f"pipeline_{name}_unversioned.pdf"
    generate_pipeline_visualisation(input_file, output_file, include_subgraphs=False)

    input_file = data_path / f"pipeline_{name}_versioned.json"
    pipeline_to_json(pipeline, input_file, versioned=True)
    output_file = f"pipeline_{name}_subgraphs_versioned.pdf"
    generate_pipeline_visualisation(input_file, output_file, include_subgraphs=True)
    output_file = f"pipeline_{name}_versioned.pdf"
    generate_pipeline_visualisation(input_file, output_file, include_subgraphs=False)
