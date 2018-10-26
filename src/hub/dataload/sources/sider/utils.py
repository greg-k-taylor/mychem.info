

def last_element(d, key_list):
    """Return the last element and key for a document d given a docstring.

    A document d is passed with a list of keys key_list.  A generator is then
    returned for all elements that match all keys.  Not that there may be 
    a 1-to-many relationship between keys and elements due to lists in the document.

    :param d: document d to return elements from
    :param key_list: list of keys that specify elements in the document d
    :return: generator for elements that match all keys
    """
    # preconditions
    if not d or not key_list:
        return
    k = key_list.pop(0)
    # termination
    if not key_list:
        yield k, d
    # recursion
    else:
        try:
            t = d[k]
        except KeyError:
            return # key does not exist
        except TypeError:
            return # not sub-scriptable
        if isinstance(t, dict):                
            yield from last_element(t, key_list)
        elif isinstance(t, list):
            for l in t:
                yield from last_element(l, key_list.copy())
        elif isinstance(t, tuple):
            # unsupported type
            raise ValueError("unsupported type in key {}".format(k))

def key_value(dictionary, key):
    """Return a generator for all values in a dictionary specific by a dotstirng (key)

    :param dictionary: a dictionary to return values from
    :param key: key that specifies a value in the dictionary
    :return: generator for values that match the given key
    """
    def safe_ref(k, d):
        if d:
            try:
                return d[k]
            except KeyError:
                pass

    if not is_str(key):
        raise TypeError("key argument must of be of type 'str'")
    key_list = key.split('.')
    for k, le in last_element(dictionary, key_list):
        yield safe_ref(k, le)
