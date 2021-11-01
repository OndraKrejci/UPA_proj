def mergeListsByKey(l1, l2, key):
    """Merge two lists by key value
        Usage:
            l3 = [{**i1, **i2} for i1, i2 in mergeListsByKey(l1, l2, key="key")]"""
    
    # Sort by key  
    l1 = iter(sorted(l1, key=lambda x: x[key]))
    l2 = iter(sorted(l2, key=lambda x: x[key]))
    
    # Create fill values
    i1 = next(l1, None)
    f1 = i1.copy()
    f1.pop(key, None)
    f1 = dict.fromkeys(f1, None)
    
    i2 = next(l2, None)
    f2 = i2.copy()
    f2.pop(key, None)
    f2 = dict.fromkeys(f2, None)
    
    # Iterate and merge
    while (i1 is not None) or (i2 is not None):  
        if i1 is None:
            yield f1, i2
            i2 = next(l2, None)
        elif i2 is None:
            yield i1, f2
            i2 = next(l1, None)
        elif i1.get(key) == i2.get(key):
            yield i1, i2
            i1 = next(l1, None)
            i2 = next(l2, None)
        elif i1.get(key) < i2.get(key):
            yield i1, f2
            i1 = next(l1, None)
        else:
            yield f1, i2
            i2 = next(l2, None)

