import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import json

def correlation_heatmap(data_seq_ids):
    """Create a correlation heatmap showing Jaccard similarity between lists"""
    lists = {}
    for k, v in data_seq_ids.items():
        if not v:
            print(f'{k} empty, skipping')
            continue
        lists[k] = v
    names = list(lists.keys())
    n = len(names)

    corr_matrix = np.zeros((n, n))

    for i in range(n):
        for j in range(n):
            set_i = set(lists[names[i]])
            set_j = set(lists[names[j]])

            # Handle empty sets
            if not set_i and not set_j:
                # Both empty = 1.0 similarity
                corr_matrix[i, j] = 1.0
            elif not set_i or not set_j:
                # One empty, one not = 0.0 similarity
                corr_matrix[i, j] = 0.0
            else:
                # Jaccard similarity: size of intersection divided by size of union
                intersection = len(set_i.intersection(set_j))
                union = len(set_i.union(set_j))
                corr_matrix[i, j] = intersection / union if union > 0 else 0

    fig, ax = plt.subplots(figsize=(14, 14))
    sns.heatmap(corr_matrix, annot=True, cmap='viridis', xticklabels=names, yticklabels=names)
    plt.title('Jaccard Similarity')
    plt.xticks(rotation=45, ha='right')
    return fig


if __name__ == '__main__':
    with open('data_seq_ids.json') as f:
        data_seq_ids = json.loads(f.read())

    correlation_heatmap(data_seq_ids)
    plt.savefig('correlation_heatmap.png', bbox_inches='tight')
