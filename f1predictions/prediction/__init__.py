from abc import ABC, abstractmethod
from typing import Any
import numpy as np
from sklearn.model_selection import train_test_split


class AbstractBuilder(ABC):
    X: np.array
    y: np.array
    X_train: np.array
    y_train: np.array
    X_test: np.array
    y_test: np.array
    test_size: float
    regressor: Any
    random_state: int

    @abstractmethod
    def set_model(self, X, y):
        self.X = X
        self.y = y

        return self

    @abstractmethod
    def create_train_test_set(self, test_size: float, random_state: int = 1):
        self.random_state = random_state
        if test_size < 0 or test_size > 1:
            raise ValueError('Test size must be between 0 and 1')

        if self.X is None or self.y is None:
            raise ValueError('You must set model first')

        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(
            self.X, self.y, test_size=test_size, random_state=random_state
        )

        return self

    @abstractmethod
    def get(self):
        return self.regressor

    def _get_model(self):
        (X, y, X_test, y_test) = (self.X_train, self.y_train, self.X_test, self.y_test) \
            if self.X_train is not None and self.y_train is not None \
            else (self.X, self.y, self.X, self.y)

        if X is None or y is None:
            raise ValueError('Model has not been set')

        return X, y, X_test, y_test


class AbstractDecisionTreeBasedRegressorBuilder(AbstractBuilder):
    criterion: str

    @abstractmethod
    def set_criterion(self, criterion: str):
        if criterion not in ['squared_error', 'friedman_mse', 'absolute_error', 'poisson']:
            raise ValueError(
                'Criterion must be on of the: squared_error, friedman_mse, absolute_error, poisson, {} given'
                .format(criterion)
            )

        self.criterion = criterion

        return self


class AbstractDecisionTreeBasedClassifierBuilder(AbstractBuilder):
    criterion: str

    @abstractmethod
    def set_criterion(self, criterion: str):
        if criterion not in ['squared_error', 'friedman_mse', 'absolute_error', 'poisson']:
            raise ValueError(
                'Criterion must be on of the: squared_error, friedman_mse, absolute_error, poisson, {} given'
                .format(criterion)
            )

        self.criterion = criterion

        return self