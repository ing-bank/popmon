import json
from itertools import cycle
from pathlib import Path

import pygraphviz as pgv


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
    subgraph_colors = cycle(colors)
    module_style = {"shape": "rectangle", "fillcolor": "chartreuse", "style": "filled"}
    dataset_style = {"shape": "oval", "fillcolor": "orange", "style": "filled"}
    subgraph_style = {}
    edge_style = {"fontcolor": "gray50"}

    def get_module_label(module):
        label = f"<{module['name']}"
        d = module.get("desc", "")
        if len(d) > 0:
            label += f" <BR/><I>{d}</I>"
        label += ">"
        return label

    def process(data, G):
        if data["type"] == "subgraph":
            if include_subgraphs:
                c = G.add_subgraph(
                    name=f'cluster_{data["name"]}',
                    label=data["name"],
                    color=next(subgraph_colors),
                    **subgraph_style,
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
            raise ValueError("type should be 'subgraph' or 'module'")

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
    pipeline.to_json(input_file, versioned=False)
    output_file = f"pipeline_{name}_subgraphs_unversioned.pdf"
    generate_pipeline_visualisation(input_file, output_file, include_subgraphs=True)
    output_file = f"pipeline_{name}_unversioned.pdf"
    generate_pipeline_visualisation(input_file, output_file, include_subgraphs=False)

    input_file = data_path / f"pipeline_{name}_versioned.json"
    pipeline.to_json(input_file, versioned=True)
    output_file = f"pipeline_{name}_subgraphs_versioned.pdf"
    generate_pipeline_visualisation(input_file, output_file, include_subgraphs=True)
    output_file = f"pipeline_{name}_versioned.pdf"
    generate_pipeline_visualisation(input_file, output_file, include_subgraphs=False)
