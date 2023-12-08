import pandas as pd
from sklearn.model_selection import train_test_split
from delphix.core import Pipeline, Task



def train_test_data(file_name: str, test_size: float, random_state=42) -> (pd.DataFrame, pd.DataFrame):
    # load iris.csv using pandas
    data = pd.read_csv(file_name)
    # split data into train and test using scikit-learn
    train, test = train_test_split(data, test_size=test_size, random_state=random_state)
    print("train length: ", len(train), "test length: ", len(test))
    return train, test


def train_model(train: pd.DataFrame, random_state:int=42) -> object:
    # train a model using scikit-learn
    from sklearn.tree import DecisionTreeClassifier
    clf = DecisionTreeClassifier(random_state=random_state)
    clf.fit(train[['sepal_length', 'sepal_width', 'petal_length', 'petal_width']], train['species'])
    print(clf)
    return clf


def evaluate_model(model: object, test: pd.DataFrame) -> float:
    # evaluate the model using scikit-learn
    from sklearn.metrics import accuracy_score
    predictions = model.predict(test[['sepal_length', 'sepal_width', 'petal_length', 'petal_width']])
    score = accuracy_score(test['species'], predictions)
    print("Accuracy: ", score)
    return score




pipeline = Pipeline(name="pipeline_example",
             description="A pipeline example", version="1.0.0").add_tasks([
                Task(train_test_data),
                Task(train_model, inputs=['train_test_data[0]']),
                Task(evaluate_model, inputs=['train_model', 'train_test_data[1]']),
             ])


print("inferred params:", pipeline.infer_pipe_params())

pipeline.run(file_name="data/iris.csv", test_size=0.5, random_state=42)



print(__file__)
print(__module__)
