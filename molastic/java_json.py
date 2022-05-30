import json

from . import java_api


def loads(s):
    return _cast_python_object_to_java_object(json.loads(s))

def dumps(obj):
    return json.dumps(_cast_java_object_to_python_object(obj))

def _cast_python_object_to_java_object(o):
    if isinstance(o, dict):
        m = java_api.HashMap()
        for k,v in o.items():
            java_v = _cast_python_object_to_java_object(v)
            m.put(k, java_v)
        return m
    if isinstance(o, (list, tuple)):
        l = java_api.ArrayList()
        for i in o:
            java_i = _cast_python_object_to_java_object(i)
            l.add(java_i)
        return l
    if isinstance(o, (str, int, float, bool)):
        return o
    raise NotImplementedError(o)

def _cast_java_object_to_python_object(o):
    if isinstance(o, java_api.Map):
        m = dict()
        for entry in o.entrySet():
            m[_cast_java_object_to_python_object(entry.getKey())] = _cast_java_object_to_python_object(entry.getValue())
        return m
    if isinstance(o, java_api.List):
        l = list()
        for i in o:
            l.append(_cast_java_object_to_python_object(i))
        return l
    if isinstance(o, (str, java_api.String)):
        return str(o)
    if isinstance(o, (int, java_api.Integer)):
        raise int(o)
    if isinstance(o, (float, java_api.Float)):
        return float(o)
    if isinstance(o, (bool, java_api.Boolean)):
        return bool(o)
    raise NotImplementedError(o, type(o))
