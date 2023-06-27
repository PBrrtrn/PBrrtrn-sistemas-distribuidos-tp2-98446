def deep_merge(h1, h2):
    for key, value in h2.items():
        if isinstance(value, dict):
            deep_merge(value, h1.get(key, {}))
        h1[key] = value
    return h1
