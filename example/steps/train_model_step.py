import pandas as pd
from dependency_injector.wiring import inject, Provide
from delphix.core import Container



@inject
def train_model(train: pd.DataFrame, mlflow = Provide[Container.mlflow]) -> object:
    print("****", mlflow)
    print(type(mlflow))
    # train a model using scikit-learn
    from sklearn.tree import DecisionTreeClassifier
    clf = DecisionTreeClassifier(random_state=42)
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