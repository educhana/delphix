


import inspect
from typing import Dict, List



class Task:
    def __init__(self, func, name=None, inputs=None, outputs=None, dependencies=None):
        self.func = func
        self.name = name if name is not None else func.__name__
        # convert inputs to dict
        if inputs is None:
            self.inputs = {}
        elif isinstance(inputs, (list, tuple)):
            func_params = self.get_param_types()
            self.inputs = {param_name: param_input for param_name, param_input in zip(func_params.keys(), inputs)}
        elif isinstance(inputs, dict):
            self.inputs = inputs.copy()
        else:
            raise TypeError("inputs must be a list, tuple or dict")
        self.outputs = outputs
        self.dependencies = dependencies

    def get_param_types(self):
        func_signature = inspect.signature(self.func)
        func_params = {p.name: p.annotation for p in func_signature.parameters.values()}
        return func_params
    
    def get_return_type(self):
        func_signature = inspect.signature(self.func)
        func_return = func_signature.return_annotation
        return func_return

    def run(self, context_vars, task_outputs):
        # build arguments for the function
        arguments = {}
        func_signature = inspect.signature(self.func)
        func_params = func_signature.parameters
        for param_name, param in func_params.items():
            if param_name in self.inputs:
                arguments[param_name] = eval(self.inputs[param_name], task_outputs, None)  # TODO: replace eval
            else:
                var_name = resolve_var_name(context_vars, self.name, param_name)
                if var_name is not None:
                    arguments[param_name] = context_vars[var_name]
                    print(f"Resolved {self.name}::{param_name} to {var_name}")
        # call function with arguments
        result = call_func_with_arguments(self.func, arguments)
        return result




def call_func_with_arguments(func, arguments):
    # separate args and kwargs inspecting the function signature
    func_signature = inspect.signature(func)
    func_params = func_signature.parameters
    args = []
    kwargs = {}
    for param_name, param in func_params.items():
        if param_name in arguments:
            if param.kind == param.POSITIONAL_ONLY:
                args.append(arguments[param_name])
            else:
                kwargs[param_name] = arguments[param_name]
        else:
            # positional with default value. leave it as is
            if param.default is param.empty:
                #raise ValueError(f"param '{param_name}' is not bound to any value")
                pass

    # call function with args and kwargs
    return func(*args, **kwargs)


def resolve_var_name(context_vars, namespace, param_name):
    fq_param_name = f"{namespace}__{param_name}"
    if fq_param_name in context_vars:
        return fq_param_name
    if param_name in context_vars:
        return param_name
    else:
        partial_param_name = f"__{param_name}"
        for param in context_vars.keys():
            if param.endswith(partial_param_name):
                return param
    # cannot resolve parameter name
    return None


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
        task_outputs = {}  # all task outputs
        # for each task, resolve inputs and run it
        for task in self.tasks.values():
            result = task.run(kwargs, task_outputs)
            # save task output for later resolution of other tasks inputs
            task_outputs[task.name] = result


from dependency_injector import containers, providers
from dependency_injector.wiring import Provide, inject

class Container(containers.DeclarativeContainer):

    config = providers.Configuration()

    mlflow = __import__("mlflow")
    

container = Container()


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
    



