# python o ipython
import pandas as pd

nodes = pd.read_parquet("s3://osm2ohm-rub21/output/sample_001/type=node/")
ways  = pd.read_parquet("s3://osm2ohm-rub21/output/sample_001/type=way/")

print(f"Nodes: {len(nodes)}")
print(f"Ways:  {len(ways)}")
print(nodes.head())
print(ways.head())

# Tags más comunes
# `good_tags` viene como lista de structs: [{"key": "name", "value": "foo"}, ...]
from collections import Counter

def tag_keys(tags):
    if tags is None:
        return []
    if isinstance(tags, dict):
        return list(tags.keys())
    out = []
    for t in tags:
        if isinstance(t, dict):
            out.append(t.get("key"))
        elif isinstance(t, (tuple, list)):
            out.append(t[0])
        else:
            try:
                out.append(t["key"])
            except (TypeError, KeyError, IndexError):
                pass
    return [k for k in out if k is not None]

all_tags = Counter()
for tags in nodes["good_tags"]:
    all_tags.update(tag_keys(tags))
print("Top tags en nodes:")
print(all_tags.most_common(20))

print()
all_tags_w = Counter()
for tags in ways["good_tags"]:
    all_tags_w.update(tag_keys(tags))
print("Top tags en ways:")
print(all_tags_w.most_common(20))