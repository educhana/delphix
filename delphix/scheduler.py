

from typing import List, Dict, Protocol

#from delphix.core import Task
class Task: pass

class Scheduler(Protocol):

    def run_tasks(self, tasks: List[Task]):  # hummm, what should be the return type?
        ...



class LocalScheduler:
    def run_tasks(self, tasks: List[Task], **kwargs):
        task_outputs = {}  # collects all task outputs
        task_dict = {task.name: task for task in tasks}  # convert to dict for easy access
        # build the dag
        import paradag   # https://pypi.org/project/paradag/
        dag = paradag.DAG()
        # add all tasks as vertex
        for task in self.tasks.values():
            dag.add_vertex(task.name)
        # add all dependencies as edges
        for task in self.tasks.values():
            if task.dependencies:
                for task_dep in task.dependencies:
                    dag.add_edge(task_dep, task.name)
        
        class CustomExecutor:
            def param(self, vertex):
                return vertex

            def execute(self, param):
                print('Executing:', param)
                task = task_dict[param]   # get the task
                result = task.run(kwargs, task_outputs)  # run the task
                task_outputs[param] = result  # save the result
                return result

            def report_start(self, vertices):
                print('Start to run:', vertices)

            def report_running(self, vertices):
                print('Current running:', vertices)

            def report_finish(self, vertices_result):
                for vertex, result in vertices_result:
                    print('Finished running {0} with result: {1}'.format(vertex, result))        


        paradag.dag_run(dag, processor=paradag.SequentialProcessor(), executor=CustomExecutor(self.tasks))
