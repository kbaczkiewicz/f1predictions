import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
import sys
from sklearn.tree import DecisionTreeRegressor
from f1predictions.prediction import AbstractDecisionTreeBasedRegressorBuilder, AbstractBuilder


class RandomForestRegressorBuilder(AbstractDecisionTreeBasedRegressorBuilder):
    regressor: RandomForestRegressor

    def set_model(self, X, y) -> 'RandomForestRegressorBuilder':
        super().set_model(X, y)

        return self

    def set_criterion(self, criterion: str) -> 'RandomForestRegressorBuilder':
        super().set_criterion(criterion)

        return self

    def create_train_test_set(self, test_size: float, random_state: int = 1) -> 'RandomForestRegressorBuilder':
        super().create_train_test_set(test_size, random_state)

        return self

    def create_regressor(self, max_n_optimizers: int = 8) -> 'RandomForestRegressorBuilder':
        X, y, X_test, y_test = self._get_model()

        if self.criterion is None:
            raise ValueError('Criterion has not been set')

        best_regressor = RandomForestRegressor(
            criterion=self.criterion, n_estimators=1, random_state=self.random_state
        )

        best_score = -1 * sys.maxsize

        for i in range(1, max_n_optimizers):
            regressor = RandomForestRegressor(
                criterion=self.criterion, n_estimators=i, random_state=self.random_state
            )

            regressor.fit(X, y)
            score = regressor.score(X_test, y_test)
            if score > best_score:
                best_regressor, best_score = regressor, score

        self.regressor = best_regressor

        return self

    def get(self) -> RandomForestRegressor:
        return self.regressor


class DecisionTreeRegressorBuilder(AbstractDecisionTreeBasedRegressorBuilder):
    regressor: DecisionTreeRegressor

    def set_model(self, X, y) -> 'DecisionTreeRegressorBuilder':
        super().set_model(X, y)

        return self

    def set_criterion(self, criterion: str) -> 'DecisionTreeRegressorBuilder':
        super().set_criterion(criterion)

        return self

    def create_train_test_set(self, test_size: float, random_state: int = 1) -> 'DecisionTreeRegressorBuilder':
        super().create_train_test_set(test_size, random_state)

        return self

    def create_regressor(self, max_depth: int) -> 'DecisionTreeRegressorBuilder':
        X, y, X_test, y_test = self._get_model()

        if self.criterion is None:
            raise ValueError('Criterion has not been set')

        best_regressor = DecisionTreeRegressor(criterion=self.criterion, max_depth=1)

        best_score = -1 * sys.maxsize

        for i in range(1, max_depth):
            regressor = DecisionTreeRegressor(criterion=self.criterion, max_depth=i)

            regressor.fit(X, y)
            score = regressor.score(X_test, y_test)
            if score > best_score:
                best_regressor, best_score = regressor, score

        self.regressor = best_regressor

        return self

    def get(self) -> DecisionTreeRegressor:
        return self.regressor


class LinearRegressorBuilder(AbstractBuilder):
    regressor: LinearRegression

    def set_model(self, X: np.array, y: np.array) -> 'LinearRegressorBuilder':
        self.X = X
        self.y = y

        return self

    def create_train_test_set(self, test_size: float, random_state: int = 1) -> 'LinearRegressorBuilder':
        super().create_train_test_set(test_size, random_state)

        return self

    def create_regressor(self) -> 'LinearRegressorBuilder':
        X, y, X_test, y_test = self._get_model()
        self.regressor = LinearRegression()
        self.regressor.fit(X, y)

        return self

    def get(self) -> LinearRegression:
        return self.regressor
