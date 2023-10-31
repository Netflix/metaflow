# Copyright 2018 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

__all__ = [
    'ModelBase',
]

import inspect
from collections import abc, OrderedDict
from typing import Any, Callable, Dict, List, Mapping, MutableMapping, MutableSequence, Optional, Sequence, Tuple, Type, TypeVar, Union, cast, get_type_hints

T = TypeVar('T')


def verify_object_against_type(x: Any, typ: Type[T]) -> T:
    """Verifies that the object is compatible to the specified type (types from
    the typing package can be used)."""
    #TODO: Merge with parse_object_from_struct_based_on_type which has almost the same code
    if typ is type(None):
        if x is None:
            return x
        else:
            raise TypeError('Error: Object "{}" is not None.'.format(x))

    if typ is Any or type(typ) is TypeVar:
        return x

    try:  #isinstance can fail for generics
        if isinstance(x, typ):
            return cast(typ, x)
    except:
        pass

    if hasattr(typ, '__origin__'):  #Handling generic types
        if typ.__origin__ is Union:  #Optional == Union
            exception_map = {}
            possible_types = typ.__args__
            if type(
                    None
            ) in possible_types and x is None:  #Shortcut for Optional[] tests. Can be removed, but the exceptions will be more noisy.
                return x
            for possible_type in possible_types:
                try:
                    verify_object_against_type(x, possible_type)
                    return x
                except Exception as ex:
                    exception_map[possible_type] = ex
                    pass
            #exception_lines = ['Exception for type {}: {}.'.format(t, e) for t, e in exception_map.items()]
            exception_lines = [str(e) for t, e in exception_map.items()]
            exception_lines.append(
                'Error: Object "{}" is incompatible with type "{}".'.format(
                    x, typ))
            raise TypeError('\n'.join(exception_lines))

        #not Union => not None
        if x is None:
            raise TypeError(
                'Error: None object is incompatible with type {}'.format(typ))

        #assert isinstance(x, typ.__origin__)
        generic_type = typ.__origin__ or getattr(
            typ, '__extra__', None
        )  #In python <3.7 typing.List.__origin__ == None; Python 3.7 has working __origin__, but no __extra__  TODO: Remove the __extra__ once we move to Python 3.7
        if generic_type in [
                list, List, abc.Sequence, abc.MutableSequence, Sequence,
                MutableSequence
        ] and type(x) is not str:  #! str is also Sequence
            if not isinstance(x, generic_type):
                raise TypeError(
                    'Error: Object "{}" is incompatible with type "{}"'.format(
                        x, typ))
            # In Python <3.7 Mapping.__args__ is None.
            # In Python 3.9 typ.__args__ does not exist when the generic type does not have subscripts
            type_args = typ.__args__ if getattr(
                typ, '__args__', None) is not None else (Any, Any)
            inner_type = type_args[0]
            for item in x:
                verify_object_against_type(item, inner_type)
            return x

        elif generic_type in [
                dict, Dict, abc.Mapping, abc.MutableMapping, Mapping,
                MutableMapping, OrderedDict
        ]:
            if not isinstance(x, generic_type):
                raise TypeError(
                    'Error: Object "{}" is incompatible with type "{}"'.format(
                        x, typ))
            # In Python <3.7 Mapping.__args__ is None.
            # In Python 3.9 typ.__args__ does not exist when the generic type does not have subscripts
            type_args = typ.__args__ if getattr(
                typ, '__args__', None) is not None else (Any, Any)
            inner_key_type = type_args[0]
            inner_value_type = type_args[1]
            for k, v in x.items():
                verify_object_against_type(k, inner_key_type)
                verify_object_against_type(v, inner_value_type)
            return x

        else:
            raise TypeError(
                'Error: Unsupported generic type "{}". type.__origin__ or type.__extra__ == "{}"'
                .format(typ, generic_type))

    raise TypeError('Error: Object "{}" is incompatible with type "{}"'.format(
        x, typ))


def parse_object_from_struct_based_on_type(struct: Any, typ: Type[T]) -> T:
    """Constructs an object from structure (usually dict) based on type.

    Supports list and dict types from the typing package plus Optional[]
    and Union[] types. If some type is a class that has .from_dict class
    method, that method is used for object construction.
    """
    if typ is type(None):
        if struct is None:
            return None
        else:
            raise TypeError('Error: Structure "{}" is not None.'.format(struct))

    if typ is Any or type(typ) is TypeVar:
        return struct

    try:  #isinstance can fail for generics
        #if (isinstance(struct, typ)
        #    and not (typ is Sequence and type(struct) is str) #! str is also Sequence
        #    and not (typ is int and type(struct) is bool) #! bool is int
        #):
        if type(struct) is typ:
            return struct
    except:
        pass
    if hasattr(typ, 'from_dict'):
        try:  #More informative errors
            return typ.from_dict(struct)
        except Exception as ex:
            raise TypeError(
                'Error: {}.from_dict(struct={}) failed with exception:\n{}'
                .format(typ.__name__, struct, str(ex)))
    if hasattr(typ, '__origin__'):  #Handling generic types
        if typ.__origin__ is Union:  #Optional == Union
            results = {}
            exception_map = {}
            # In Python 3.9 typ.__args__ does not exist when the generic type does not have subscripts
            # Union without subscripts seems useless, but semantically it should be the same as Any.
            possible_types = list(getattr(typ, '__args__', [Any]))
            #if type(None) in possible_types and struct is None: #Shortcut for Optional[] tests. Can be removed, but the exceptions will be more noisy.
            #    return None
            #Hack for Python <3.7 which for some reason "simplifies" Union[bool, int, ...] to just Union[int, ...]
            if int in possible_types:
                possible_types = possible_types + [bool]
            for possible_type in possible_types:
                try:
                    obj = parse_object_from_struct_based_on_type(
                        struct, possible_type)
                    results[possible_type] = obj
                except Exception as ex:
                    if isinstance(ex, TypeError):
                        exception_map[possible_type] = ex
                    else:
                        exception_map[
                            possible_type] = 'Unexpected exception when trying to convert structure "{}" to type "{}": {}: {}'.format(
                                struct, typ, type(ex), ex)
                    pass

            #Single successful parsing.
            if len(results) == 1:
                return list(results.values())[0]

            if len(results) > 1:
                raise TypeError(
                    'Error: Structure "{}" is ambiguous. It can be parsed to multiple types: {}.'
                    .format(struct, list(results.keys())))

            exception_lines = [str(e) for t, e in exception_map.items()]
            exception_lines.append(
                'Error: Structure "{}" is incompatible with type "{}" - none of the types in Union are compatible.'
                .format(struct, typ))
            raise TypeError('\n'.join(exception_lines))
        #not Union => not None
        if struct is None:
            raise TypeError(
                'Error: None structure is incompatible with type {}'.format(
                    typ))

        #assert isinstance(x, typ.__origin__)
        generic_type = typ.__origin__ or getattr(
            typ, '__extra__', None
        )  #In python <3.7 typing.List.__origin__ == None; Python 3.7 has working __origin__, but no __extra__  TODO: Remove the __extra__ once we move to Python 3.7
        if generic_type in [
                list, List, abc.Sequence, abc.MutableSequence, Sequence,
                MutableSequence
        ] and type(struct) is not str:  #! str is also Sequence
            if not isinstance(struct, generic_type):
                raise TypeError(
                    'Error: Structure "{}" is incompatible with type "{}" - it does not have list type.'
                    .format(struct, typ))
            # In Python <3.7 Mapping.__args__ is None.
            # In Python 3.9 typ.__args__ does not exist when the generic type does not have subscripts
            type_args = typ.__args__ if getattr(
                typ, '__args__', None) is not None else (Any, Any)
            inner_type = type_args[0]
            return [
                parse_object_from_struct_based_on_type(item, inner_type)
                for item in struct
            ]

        elif generic_type in [
                dict, Dict, abc.Mapping, abc.MutableMapping, Mapping,
                MutableMapping, OrderedDict
        ]:  #in Python <3.7 there is a difference between abc.Mapping and typing.Mapping
            if not isinstance(struct, generic_type):
                raise TypeError(
                    'Error: Structure "{}" is incompatible with type "{}" - it does not have dict type.'
                    .format(struct, typ))
            # In Python <3.7 Mapping.__args__ is None.
            # In Python 3.9 typ.__args__ does not exist when the generic type does not have subscripts
            type_args = typ.__args__ if getattr(
                typ, '__args__', None) is not None else (Any, Any)
            inner_key_type = type_args[0]
            inner_value_type = type_args[1]
            return {
                parse_object_from_struct_based_on_type(k, inner_key_type):
                parse_object_from_struct_based_on_type(v, inner_value_type)
                for k, v in struct.items()
            }

        else:
            raise TypeError(
                'Error: Unsupported generic type "{}". type.__origin__ or type.__extra__ == "{}"'
                .format(typ, generic_type))

    raise TypeError(
        'Error: Structure "{}" is incompatible with type "{}". Structure is not the instance of the type, the type does not have .from_dict method and is not generic.'
        .format(struct, typ))


def convert_object_to_struct(obj, serialized_names: Mapping[str, str] = {}):
    """Converts an object to structure (usually a dict).

    Serializes all properties that do not start with underscores. If the
    type of some property is a class that has .to_dict class method,
    that method is used for conversion. Used by the ModelBase class.
    """
    signature = inspect.signature(obj.__init__)  #Needed for default values
    result = {}
    for python_name in signature.parameters:  #TODO: Make it possible to specify the field ordering regardless of the presence of default values
        value = getattr(obj, python_name)
        if python_name.startswith('_'):
            continue
        attr_name = serialized_names.get(python_name, python_name)
        if hasattr(value, "to_dict"):
            result[attr_name] = value.to_dict()
        elif isinstance(value, list):
            result[attr_name] = [
                (x.to_dict() if hasattr(x, 'to_dict') else x) for x in value
            ]
        elif isinstance(value, dict):
            result[attr_name] = {
                k: (v.to_dict() if hasattr(v, 'to_dict') else v)
                for k, v in value.items()
            }
        else:
            param = signature.parameters.get(python_name, None)
            if param is None or param.default == inspect.Parameter.empty or value != param.default:
                result[attr_name] = value

    return result


def parse_object_from_struct_based_on_class_init(
        cls: Type[T],
        struct: Mapping,
        serialized_names: Mapping[str, str] = {}) -> T:
    """Constructs an object of specified class from structure (usually dict)
    using the class.__init__ method. Converts all constructor arguments to
    appropriate types based on the __init__ type hints. Used by the ModelBase
    class.

    Arguments:

    serialized_names: specifies the mapping between __init__ parameter names and the structure key names for cases where these names are different (due to language syntax clashes or style differences).
    """
    parameter_types = get_type_hints(
        cls.__init__)  #Properlty resolves forward references

    serialized_names_to_pythonic = {v: k for k, v in serialized_names.items()}
    #If a pythonic name has a different original name, we forbid the pythonic name in the structure. Otherwise, this function would accept "python-styled" structures that should be invalid
    forbidden_struct_keys = set(
        serialized_names_to_pythonic.values()).difference(
            serialized_names_to_pythonic.keys())
    args = {}
    for original_name, value in struct.items():
        if original_name in forbidden_struct_keys:
            raise ValueError(
                'Use "{}" key instead of pythonic key "{}" in the structure: {}.'
                .format(serialized_names[original_name], original_name, struct))
        python_name = serialized_names_to_pythonic.get(original_name,
                                                       original_name)
        param_type = parameter_types.get(python_name, None)
        if param_type is not None:
            args[python_name] = parse_object_from_struct_based_on_type(
                value, param_type)
        else:
            args[python_name] = value

    return cls(**args)


class ModelBase:
    """Base class for types that can be converted to JSON-like dict structures
    or constructed from such structures. The object fields, their types and
    default values are taken from the __init__ method arguments. Override the
    _serialized_names mapping to control the key names of the serialized
    structures.

    The derived class objects will have the .from_dict and .to_dict methods for conversion to or from structure. The base class constructor accepts the arguments map, checks the argument types and sets the object field values.

    Example derived class:

    class TaskSpec(ModelBase):
        _serialized_names = {
            'component_ref': 'componentRef',
            'is_enabled': 'isEnabled',
        }

        def __init__(self,
            component_ref: ComponentReference,
            arguments: Optional[Mapping[str, ArgumentType]] = None,
            is_enabled: Optional[Union[ArgumentType, EqualsPredicate, NotEqualsPredicate]] = None, #Optional property with default value
        ):
            super().__init__(locals()) #Calling the ModelBase constructor to check the argument types and set the object field values.

    task_spec = TaskSpec.from_dict("{'componentRef': {...}, 'isEnabled: {'and': {...}}}") # = instance of TaskSpec
    task_struct = task_spec.to_dict() #= "{'componentRef': {...}, 'isEnabled: {'and': {...}}}"
    """
    _serialized_names = {}

    def __init__(self, args):
        parameter_types = get_type_hints(self.__class__.__init__)
        field_values = {
            k: v
            for k, v in args.items()
            if k != 'self' and not k.startswith('_')
        }
        for k, v in field_values.items():
            parameter_type = parameter_types.get(k, None)
            if parameter_type is not None:
                try:
                    verify_object_against_type(v, parameter_type)
                except Exception as e:
                    raise TypeError(
                        'Argument for {} is not compatible with type "{}". Exception: {}'
                        .format(k, parameter_type, e))
        self.__dict__.update(field_values)

    @classmethod
    def from_dict(cls: Type[T], struct: Mapping) -> T:
        return parse_object_from_struct_based_on_class_init(
            cls, struct, serialized_names=cls._serialized_names)

    def to_dict(self) -> Mapping:
        return convert_object_to_struct(
            self, serialized_names=self._serialized_names)

    def _get_field_names(self):
        return list(inspect.signature(self.__init__).parameters)

    def __repr__(self):
        return self.__class__.__name__ + '(' + ', '.join(
            param + '=' + repr(getattr(self, param))
            for param in self._get_field_names()) + ')'

    def __eq__(self, other):
        return self.__class__ == other.__class__ and {
            k: getattr(self, k) for k in self._get_field_names()
        } == {k: getattr(other, k) for k in other._get_field_names()}

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(repr(self))
