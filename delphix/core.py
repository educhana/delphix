


import inspect
from typing import Dict, List
from .config import config





def extract_dependency_name(expr: str) -> str:
    # regex to extract valid python identifier from start of string
    import re
    match = re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*", expr)
    if match is None:
        raise ValueError(f"Invalid dependency expression '{expr}'")
    return match.group(0)


class Task:
    def __init__(self, func, name=None, inputs=None, dependencies:List[str]=None):
        self.func = func
        self.name = name if name is not None else func.__name__
        self.dependencies = dependencies if dependencies is not None else []
        # convert inputs to dict
        self.inputs = {}
        if inputs: 
            if isinstance(inputs, (list, tuple)):
                func_params = self.get_param_types()
                for param_name, param_input in zip(func_params.keys(), inputs):
                    self.inputs[param_name] = param_input
                    self.dependencies.append(extract_dependency_name(param_input))
            elif isinstance(inputs, dict):
                self.inputs =  inputs.copy()
                for param_input in inputs.values():
                    self.dependencies.append(extract_dependency_name(param_input))
            else:
                raise TypeError("inputs must be a list, tuple or dict")


    def get_param_types(self):
        func_signature = inspect.signature(self.func)
        func_params = {p.name: p.annotation for p in func_signature.parameters.values()}
        return func_params
    
    def get_return_type(self):
        func_signature = inspect.signature(self.func)
        func_return = func_signature.return_annotation
        return func_return

    def run(self, context_vars, task_outputs):
        # build arguments for the function, 
        args = []   
        kwargs = {}
        # for each param of the function
        for param_name, param in inspect.signature(self.func).parameters.items():
            param_value = inspect.Parameter.empty  # mark there is no value yet
            fq_param_name = f"{self.name}__{param_name}"  # fully qualified param name
            if param_name in self.inputs:  # first check if param is an input from other tasks
                param_value = eval(self.inputs[param_name], task_outputs, None)  # TODO: replace eval
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
        result = self.func(*args, **kwargs)
        return result









class Pipeline:
    def __init__(self, name, description, version):
        self.name = name
        self.description = description
        self.version = version
        self.tasks: Dict[str, Task] = {}
    
    def add_task(self, task: Task):
        self.tasks[task.name] = task
        # return self for chaining
        return self


    def add_tasks(self, tasks: List[Task]):
        for task in tasks:
            self.add_task(task)
        # return self for chaining
        return self


    def infer_pipe_params(self):
        inferred_pipe_params = {}
        for task in self.tasks.values():
            # get all params of the task function
            func_signature = inspect.signature(task.func)
            func_params = func_signature.parameters
            #check all params not in inputs of the task
            task_inputs = task.inputs
            for param_name in func_params.keys():
                if param_name not in task_inputs:
                    # add fully qualified param name to inferred params
                    fq_param_name = f"{task.name}__{param_name}"
                    inferred_pipe_params[fq_param_name] = func_params[param_name]

        return inferred_pipe_params

    def run(self, **kwargs):
        scheduler = config.resolve_dependency("scheduler")
        return scheduler.run_tasks(self.tasks.values(), **kwargs)










# main
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Run a pipeline')
    parser.add_argument('pipeline', type=str, help='pipeline to run')
    args = parser.parse_args()
    print(f"Running pipeline '{args.pipeline}'")
    # load the module containing the pipeline
    pipeline_module = __import__(args.pipeline)
    print(pipeline_module)
    # get pipeline object
    pipeline = pipeline_module.pipeline
    print(pipeline)
    



