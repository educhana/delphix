from delphix.config import config
from delphix.core import extract_dependency_name


import inspect
from typing import List


class Task:
    def __init__(self, func, name=None, inputs=None, dependencies:List[str]=None):
        self._func = func
        self._name = name if name is not None else func.__name__
        self._dependencies = dependencies if dependencies is not None else []
        # convert inputs to dict
        self._inputs = {}
        if inputs:
            if isinstance(inputs, (list, tuple)):
                func_params = self.get_param_types()
                for param_name, param_input in zip(func_params.keys(), inputs):
                    self._inputs[param_name] = param_input
                    self._dependencies.append(extract_dependency_name(param_input))
            elif isinstance(inputs, dict):
                self._inputs =  inputs.copy()
                for param_input in inputs.values():
                    self._dependencies.append(extract_dependency_name(param_input))
            else:
                raise TypeError("inputs must be a list, tuple or dict")

    @property
    def name(self):
        return self._name

    @property
    def dependencies(self):
        return self._dependencies

    @property
    def inputs(self):
        return self._inputs


    def get_param_types(self):
        func_signature = inspect.signature(self._func)
        func_params = {p.name: p.annotation for p in func_signature.parameters.values()}
        return func_params

    def get_return_type(self):
        func_signature = inspect.signature(self._func)
        func_return = func_signature.return_annotation
        return func_return

    def run(self, context_vars, task_outputs):
        # build arguments for the function, 
        args = []
        kwargs = {}
        # for each param of the function
        for param_name, param in inspect.signature(self._func).parameters.items():
            param_value = inspect.Parameter.empty  # mark there is no value yet
            fq_param_name = f"{self._name}__{param_name}"  # fully qualified param name
            if param_name in self._inputs:  # first check if param is an input from other tasks
                param_value = eval(self._inputs[param_name], task_outputs, None)  # TODO: replace eval
            elif fq_param_name in context_vars: # now if fully qualified param name is in context vars
                    param_value = context_vars[fq_param_name]
            elif param_name in context_vars:    # now if param name is in context vars
                    param_value = context_vars[param_name]
            # if not value, check in config dependencies
            if param_value is inspect.Parameter.empty:
                if param_name in config._vars:
                    param_value = config._vars[param_name]
            # if we have a value, check if its positional or keyword
            if param_value is not inspect.Parameter.empty:
                if param.kind == param.POSITIONAL_ONLY:
                    args.append(param_value)
                else:
                    kwargs[param_name] = param_value
        # now call the function
        result = self._func(*args, **kwargs)
        return result