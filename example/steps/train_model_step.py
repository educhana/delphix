import pandas as pd

import mlflow



def train_model(train: pd.DataFrame) -> object:
    with mlflow.start_run():
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