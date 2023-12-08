import pandas as pd
from sklearn.model_selection import train_test_split


def train_test_data(file_name: str, test_size: float, random_state=42) -> (pd.DataFrame, pd.DataFrame):
    # load iris.csv using pandas
    data = pd.read_csv(file_name)
    # split data into train and test using scikit-learn
    train, test = train_test_split(data, test_size=test_size, random_state=random_state)
    print("train length: ", len(train), "test length: ", len(test))
    return train, test