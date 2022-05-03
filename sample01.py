import json
import urllib.parse
from sqloxide import parse_sql


def encode_sql(sql):
    safe = ":/?#[]@!$&'()*+,;= \n\r\t\"^|`{}<>"
    return (
        urllib.parse.quote(sql, safe=safe)
        .replace("%", "___PERCENT01___")
        .replace("___PERCENT01___25", "%")
    )


def decode_dict(d):
    escaped_d = json.dumps(d).replace("%", "___PERCENT02___")
    decoded_d = urllib.parse.unquote(escaped_d.replace("___PERCENT01___", "%"))
    return json.loads(decoded_d.replace("___PERCENT02___", "%"))


def _inner_collect_src(src_objs):
    src = []
    for src_obj in src_objs:
        # print(json.dumps(src_obj))
        if "Table" in src_obj["relation"]:
            src.append(
                ".".join([x["value"] for x in src_obj["relation"]["Table"]["name"]])
            )
        elif "Derived" in src_obj["relation"]:
            src.extend(analyze_subquery(src_obj["relation"]["Derived"]["subquery"]))
    return src


def analyze_subquery(query):
    with_list = []
    if query["with"]:
        for w in query["with"]["cte_tables"]:
            with_list.append(analyze_with(w))
    src = expand_with(analyze_select(query["body"]["Select"]), with_list)
    return src


def analyze_with(query):
    # return {"name": "w_1", "src": ["schema.phys_b"]}
    name = query["alias"]["name"]["value"]
    src = analyze_subquery(query["query"])
    return {
        "name": name,
        "src": src,
    }


def analyze_select(query):
    src = []
    # print(json.dumps(query))
    from_obj = query["from"]
    src.extend(_inner_collect_src(from_obj))
    join_obj = from_obj[0]["joins"]
    src.extend(_inner_collect_src(join_obj))
    return src


def analyze_insert(query):
    dst = [".".join([x["value"] for x in query["table_name"]])]
    src = analyze_select(query["source"]["body"]["Select"])
    return {"dst": dst, "src": src}


def analyze_create_table(query):
    dst = [".".join([x["value"] for x in query["name"]])]
    print(f"calc_relation({json.dumps(query['query'])})")
    src = calc_relation(query["query"]["body"])
    return {"dst": dst, "src": src}


def analyze_body(query):
    if "Insert" in query and "Insert" in query["Insert"]:
        return analyze_insert(query["Insert"]["Insert"])
    elif "Insert" in query:
        return analyze_insert(query["Insert"])
    elif "CreateTable" in query:
        return analyze_create_table(query["CreateTable"])
    elif "Select" in query:
        return {"dst": None, "src": analyze_select(query["Select"])}


def expand_with(src, with_list):
    if src is None:
        return None
    # print(src)
    # print(with_list)
    # return ["schema.phys_c", "schema.phys_d"]
    with_dict = {x["name"]: x["src"] for x in with_list}
    new_src = []
    for s in src:
        if s in with_dict:
            new_src.extend(with_dict[s])
        else:
            new_src.append(s)
    # print(new_src)
    return new_src


def calc_relation_cte(query):
    print(json.dumps(query))
    with_list = []
    if query["with"]:
        for w in query["with"]["cte_tables"]:
            with_list.append(analyze_with(w))
    # print(json.dumps(with_list))
    relation = analyze_body(query["body"])
    # print(json.dumps(relation))
    relation["src"] = expand_with(relation["src"], with_list)
    # print(json.dumps(relation))
    return relation


def calc_relation(query):
    if "Query" in query:
        with_clause = {"with": query["Query"]["with"]}
        if "body" in query["Query"]:
            query = query["Query"]
            query.update(with_clause)
            return calc_relation_cte(query)
    elif "Insert" in query:
        return calc_relation_cte({"with": None, "body": query})
    elif "CreateTable" in query:
        return calc_relation_cte({"with": None, "body": query})


def get_relations(sql, dialect="postgres"):
    # クエリオブジェクトの配列
    queries = decode_dict(parse_sql(sql=encode_sql(sql), dialect=dialect))
    print(json.dumps(queries))
    return [calc_relation(query) for query in queries]

    # return [
    #    {"dst": ["schema.phys_a"], "src": ["schema.phys_b"]},
    #    {"dst": ["schema.phys_b"], "src": ["schema.phys_c", "schema.phys_d"]},
    # ]


def get_parents(table, relations):
    return {
        "name": "schema.phys_b",
        "parents": [
            {"name": "schema.phys_c", "parents": []},
            {"name": "schema.phys_d", "parents": []},
        ],
    }


def get_children(table, relations):
    return {
        "name": "schema.phys_b",
        "children": [
            {"name": "schema.phys_a", "children": []},
        ],
    }


"""
def print_relations(sql):
    relations = get_relations(sql)
    print(json.dumps(relations))
    # for relation in relations:
    # for dst in relation["dst"]:
    # for src in relation["src"]:
    # print(f"{dst}\t<-\t{src}")
"""


def calc_family_tree(table, sql):
    data = {}
    relations = get_relations(sql)
    data.update(get_parents(table, relations))
    data.update(get_children(table, relations))
    return data


# print(json.dumps(get_relations(sql)))
