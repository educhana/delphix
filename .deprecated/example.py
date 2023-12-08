import pandas as pd
from sklearn.model_selection import train_test_split
from delphix.core import *



@task()
def train_test_data(file_name: str) -> (pd.DataFrame, pd.DataFrame):
    # load iris.csv using pandas
    data = pd.read_csv(file_name)
    # split data into train and test using scikit-learn
    train, test = train_test_split(data, test_size=0.4, random_state=42)
    return train, test


@task()
def train_model(train: pd.DataFrame) -> object:
    # train a model using scikit-learn
    from sklearn.tree import DecisionTreeClassifier
    clf = DecisionTreeClassifier()
    clf.fit(train[['sepal_length', 'sepal_width', 'petal_length', 'petal_width']], train['species'])
    return clf

@task()
def evaluate_model(model: object, test: pd.DataFrame) -> float:
    # evaluate the model using scikit-learn
    from sklearn.metrics import accuracy_score
    predictions = model.predict(test[['sepal_length', 'sepal_width', 'petal_length', 'petal_width']])
    return accuracy_score(test['species'], predictions)


def train_pipeline():
    (train, test) = train_test_data('data/iris.csv')
    model = train_model(train)
    accuracy = evaluate_model(model, test)
    print(accuracy)


if __name__ == '__main__':
    train_pipeline()