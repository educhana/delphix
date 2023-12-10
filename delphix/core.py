


import inspect
from typing import Dict, List, Protocol

from delphix.naive import Task
from .config import config





def extract_dependency_name(expr: str) -> str:
    # regex to extract valid python identifier from start of string
    import re
    match = re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*", expr)
    if match is None:
        raise ValueError(f"Invalid dependency expression '{expr}'")
    return match.group(0)


class TaskProtocol(Protocol):
    @property
    def name(self) -> str:
        ...

    @property
    def dependencies(self) -> List[str]:
        ...

    @property
    def inputs(self) -> Dict[str, str]:
        ...

    def run(self, context_vars: Dict[str, str], task_outputs: Dict[str, str]) -> None:
        ...

    



class Pipeline:
    def __init__(self, name, description, version):
        self.name = name
        self.description = description
        self.version = version
        self.tasks: Dict[str, Task] = {}
    
    def add_task(self, task: Task):
        self.tasks[task._name] = task
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
            func_signature = inspect.signature(task._func)
            func_params = func_signature.parameters
            #check all params not in inputs of the task
            task_inputs = task._inputs
            for param_name in func_params.keys():
                if param_name not in task_inputs:
                    # add fully qualified param name to inferred params
                    fq_param_name = f"{task._name}__{param_name}"
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
    



