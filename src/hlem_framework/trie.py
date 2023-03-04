def build_trie(hle_paths):
    root = dict()
    for i, hle_path in enumerate(hle_paths):
        node = root
        for hle_id in hle_path:
            if hle_id not in node:
                new_node = dict()
                node[hle_id] = new_node
            node = node[hle_id]
    return root


def find_leaves(trie):
    leaves = []
    for key, subtrie in trie.items():
        if not subtrie:
            leaves.append([key])

        subleaves = find_leaves(subtrie)
        for leaf in subleaves:
            leaves.append([key, *leaf])
    return leaves


if __name__ == '__main__':
    trie = build_trie([
        [1, 2, 3],
        [1, 2],
        [1, 2, 3],
        [2, 4],
        [1, 2, 4]
    ])
    leaves = find_leaves(trie)
    print(leaves)
