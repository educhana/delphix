from delphix.core import Pipeline, Task, config
from example.steps.load_data_step import train_test_data
from example.steps.train_model_step import train_model, evaluate_model





pipeline = Pipeline(name="pipeline_example",
             description="A pipeline example", version="1.0.0").add_tasks([
                Task(train_test_data),
                Task(train_model, inputs=['train_test_data[0]']),
                Task(evaluate_model, inputs=['train_model', 'train_test_data[1]']),
             ])




if __name__ == "__main__":
   import delphix.loader as loader
   instance = loader.instantiate_class_from_yaml("./example.yaml")
   print(instance)
   print("inferred params:", pipeline.infer_pipe_params())
   pipeline.run(file_name="data/iris.csv", test_size=0.5, random_state=42)

    