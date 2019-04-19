import json
import codecs

file_name = "/Users/zyf/development/search-ltr/train/xgb-mode2.json"

data = []
with open(file_name, 'r') as f:
    text = f.readlines()


s ="\n".join(text)
#print(text)
json_model = json.loads(s)


def right_nodes(node_left, node_right, yes_id, no_id):
    if node_left["nodeid"] == yes_id:
        return node_left, node_right
    return node_right, node_left


def copy_node(node):
    if "leaf" in node:
        new_node = {
            "nodeid": node["nodeid"],
            "leaf": node["leaf"]
        }
        # print(node["leaf"])
        return new_node

    new_node = {
        "nodeid": 0,
        "depth": 0,
        "split": "",
        "split_condition": 0.5,
        "yes": 1,
        "no": 2,
        "missing": 1,
        "children": []}

    # print(node["split"], "nodeid", node["nodeid"])

    for key in node:
        if key != "children":
            new_node[key] = node[key]
            continue
    if "split_condition" not in node:
        new_node["split_condition"] = 0.5

    return new_node


def format_json_model(json_model, new_tree, yes_id=None, no_id=None):

        node_left = json_model[0]
        node_right = json_model[1]
        left, right = right_nodes(node_left, node_right, yes_id, no_id)

        new_tree.append(copy_node(left))
        new_tree.append(copy_node(right))

        if "children" in left and "children" in new_tree[0]:
            format_json_model(left["children"], new_tree[0]["children"], left["yes"], left["no"])
        if "children" in right and "children" in new_tree[1]:
            format_json_model(right["children"], new_tree[1]["children"], right["yes"], right["no"])


new_tree1 = []

for node in json_model:
    tmp_node = copy_node(node)
    new_tree1.append(tmp_node)
    format_json_model(node["children"], tmp_node["children"], node["yes"], node["no"])

with open("xgboost_model.json", "w") as f:
    #json_dump = json.dumps(new_tree1)
    for node in new_tree1:
        print node
    f.writelines(json.dumps(new_tree1))
