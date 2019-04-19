import json

input_file = "/Users/zyf/development/search-ltr/train/xgb-model.json"
output_file = "xgboost-model.json"

with open(input_file, "r", encoding="utf-8-sig") as f:
    text = f.readlines()

s = "\n".join(text)
json_model = json.loads(s)


def right_nodes(node_left, node_right, yes_id, no_id):
    if node_left["nodeid"] == yes_id:
        return node_left, node_right


    node_left["nodeid"] = yes_id
    node_right["nodeid"] = no_id
    return node_right, node_left


# 左子树对应 no 子树,右子树是 yes 子树。
def get_right_children_nodes_when_node_is_discrete(node_left, node_right, yes_id, no_id):
    if node_left["nodeid"] == yes_id:
        node_left["nodeid"] = no_id
        node_right["nodeid"] = yes_id

        return node_right, node_left
    return node_left, node_right


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

    for key in new_node:
        if key in node and key != "children":
            new_node[key] = node[key]
            continue
    if "split_condition" not in node:
        new_node["split_condition"] = 0.5
    # print(new_node)
    return new_node


def get_children(cur_node, child_node_id):
    children_nodes = cur_node["children"]
    for child in children_nodes:
        if child["nodeid"] == child_node_id:
            return child
    raise ValueError("没有找到nodeid:" + str(child_node_id))


def format_json_model(cur_root_node, new_tree, yes_id=None, no_id=None):
    if isinstance(cur_root_node, dict) and isinstance(new_tree, dict):
        if "children" not in cur_root_node:
            return
        if len(cur_root_node["children"]) < 1:
            raise ValueError(json_model)
        new_tree_children = new_tree["children"]

        node_left = get_children(cur_root_node, cur_root_node["yes"])  # cur_root_node["children"][0]
        node_right = get_children(cur_root_node, cur_root_node["no"])  # cur_root_node["children"][1]

        if "split_condition" in cur_root_node:
            left, right = right_nodes(node_left, node_right, yes_id, no_id)
        else:
            new_tree["split_condition"] = 0.5
            left, right = get_right_children_nodes_when_node_is_discrete(node_left, node_right, yes_id, no_id)

        new_tree_children.append(copy_node(left))
        new_tree_children.append(copy_node(right))

        if "children" in left:
            format_json_model(left, new_tree_children[0], left["yes"], left["no"])
        if "children" in right:
            format_json_model(right, new_tree_children[1], right["yes"], right["no"])


new_tree_array = []

for root_node in json_model:
    new_tree = copy_node(root_node)
    # print(new_tree["split"])
    new_tree_array.append(new_tree)
    format_json_model(root_node, new_tree, root_node["yes"], root_node["no"])

with open(output_file, "w") as f:
    f.writelines(json.dumps(new_tree_array, indent=True))
