import json
from pathlib import Path

import networkx as nx
import pygraphviz
from networkx.drawing.nx_agraph import to_agraph


def generate_pipeline_vizualisation(input_file, output_file, include_subgraphs: bool = False, include_labels: bool = False):
    data = Path(input_file).read_text()
    data = json.loads(data)

    subgraphs = []
    modules = []

    def populate(item):
        if item['type'] == 'subgraph':
            mods = []
            for m in item['modules']:
                mods += populate(m)

            subgraphs.append(
                {
                    'modules': mods,
                    'name': item['name']
                }
            )
            return mods
        elif item['type'] == 'module':
            modules.append(item)
            name = f"{item['name']}_{item['i']}"
            return [name]+list(item["out"].values())
        else:
            raise ValueError()

    populate(data)

    G = nx.DiGraph()
    for module in modules:
        label = f"<{module['name']}"
        d = module.get('desc', '')
        if len(d) > 0:
            label += f" <BR/><I>{d}</I>"
        label += ">"

        # unique name
        name = f"{module['name']}_{module['i']}"

        G.add_node(name, shape='rectangle', fillcolor='chartreuse', style='filled', label=label)


        for k, v in module['in'].items():
            kwargs = {}
            if include_labels:
                kwargs['headlabel'] = k
            G.add_edge(v, name, **kwargs)
        for k, v in module['out'].items():
            kwargs = {}
            if include_labels:
                kwargs['taillabel'] = k
            G.add_edge(name, v, **kwargs)

    # set defaults
    G.graph['graph'] = {'rankdir':'TD'}
    G.graph['node'] = {'shape':'oval', 'fillcolor': 'orange', 'style': 'filled'}
    G.graph['edge'] = {'fontcolor':"gray50"}

    A = to_agraph(G)
    if include_subgraphs:
        for idx, subgraph in enumerate(subgraphs):
            H = A.subgraph(subgraph["modules"], name=f'cluster_{idx}_{subgraph["name"].lower().replace(" ", "_")}')
            H.graph_attr["color"] = "blue"
            H.graph_attr["label"] = subgraph["name"]
            H.graph_attr["style"] = "dotted"

    A.layout('dot')
    A.draw(output_file)


if __name__ == "__main__":
    data_path = Path("<...>")

    input_file = data_path / "pipeline_self_reference_unversioned.json"
    output_file = 'popmon-report-pipeline-subgraphs-unversioned.pdf'
    generate_pipeline_vizualisation(input_file, output_file, include_subgraphs=True)

    input_file = data_path / "pipeline_self_reference_unversioned.json"
    output_file = 'popmon-report-pipeline-unversioned.pdf'
    generate_pipeline_vizualisation(input_file, output_file, include_subgraphs=False)

    input_file = data_path / "pipeline_self_reference_versioned.json"
    output_file = 'popmon-report-pipeline-subgraphs-versioned.pdf'
    generate_pipeline_vizualisation(input_file, output_file, include_subgraphs=True)

    input_file = data_path / "pipeline_self_reference_versioned.json"
    output_file = 'popmon-report-pipeline-versioned.pdf'
    generate_pipeline_vizualisation(input_file, output_file, include_subgraphs=False)
