import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
import sys

from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from f1predictions.prediction import AbstractBuilder, AbstractDecisionTreeBasedClassifierBuilder
from lightgbm import LGBMClassifier


class RandomForestClassifierBuilder(AbstractDecisionTreeBasedClassifierBuilder):
    classifier: RandomForestClassifier

    def set_model(self, X, y) -> 'RandomForestClassifierBuilder':
        super().set_model(X, y)

        return self

    def set_criterion(self, criterion: str) -> 'RandomForestClassifierBuilder':
        super().set_criterion(criterion)

        return self

    def create_train_test_set(self, test_size: float, random_state: int = 1) -> 'RandomForestClassifierBuilder':
        super().create_train_test_set(test_size, random_state)

        return self

    def create_classifier(self, max_n_optimizers: int = 8) -> 'RandomForestClassifierBuilder':
        X, y, X_test, y_test = self._get_model()

        if self.criterion is None:
            raise ValueError('Criterion has not been set')

        best_classifier = RandomForestClassifier(
            criterion=self.criterion, n_estimators=1, random_state=self.random_state
        )

        best_score = -1 * sys.maxsize

        for i in range(1, max_n_optimizers):
            Classifier = RandomForestClassifier(
                criterion=self.criterion, n_estimators=i, random_state=self.random_state
            )

            Classifier.fit(X, y)
            score = Classifier.score(X_test, y_test)
            if score > best_score:
                best_classifier, best_score = Classifier, score

        self.classifier = best_classifier

        return self

    def get(self) -> RandomForestClassifier:
        return self.classifier


class DecisionTreeClassifierBuilder(AbstractDecisionTreeBasedClassifierBuilder):
    classifier: DecisionTreeClassifier

    def set_model(self, X, y) -> 'DecisionTreeClassifierBuilder':
        super().set_model(X, y)

        return self

    def set_criterion(self, criterion: str) -> 'DecisionTreeClassifierBuilder':
        super().set_criterion(criterion)

        return self

    def create_train_test_set(self, test_size: float, random_state: int = 1) -> 'DecisionTreeClassifierBuilder':
        super().create_train_test_set(test_size, random_state)

        return self

    def create_classifier(self, max_depth: int) -> 'DecisionTreeClassifierBuilder':
        X, y, X_test, y_test = self._get_model()

        if self.criterion is None:
            raise ValueError('Criterion has not been set')

        best_classifier = DecisionTreeClassifier(criterion=self.criterion, max_depth=1)

        best_score = -1 * sys.maxsize

        for i in range(1, max_depth):
            classifier = DecisionTreeClassifier(criterion=self.criterion, max_depth=i)

            classifier.fit(X, y)
            score = classifier.score(X_test, y_test)
            if score > best_score:
                best_classifier, best_score = classifier, score

        self.classifier = best_classifier

        return self

    def get(self) -> DecisionTreeClassifier:
        return self.classifier


class LogisticRegressionClassifierBuilder(AbstractBuilder):
    classifier: LogisticRegression

    def set_model(self, X: np.array, y: np.array) -> 'LogisticRegressionClassifierBuilder':
        self.X = X
        self.y = y

        return self

    def create_train_test_set(self, test_size: float, random_state: int = 1) -> 'LogisticRegressionClassifierBuilder':
        super().create_train_test_set(test_size, random_state)

        return self

    def create_classifier(self, multi_class: str = None, solver: str = None) -> 'LogisticRegressionClassifierBuilder':
        X, y, X_test, y_test = self._get_model()
        if multi_class is not None and solver is not None:
            self.classifier = LogisticRegression(multi_class=multi_class, solver=solver)
        else:
            self.classifier = LogisticRegression()
        self.classifier.fit(X, y)

        return self

    def get(self) -> LogisticRegression:
        return self.classifier


class LightGBMClassifierBuilder(AbstractBuilder):
    classifier: LGBMClassifier

    def set_model(self, X, y) -> 'LightGBMClassifierBuilder':
        super().set_model(X, y)

        return self

    def create_train_test_set(self, test_size: float, random_state: int = 1) -> 'LightGBMClassifierBuilder':
        super().create_train_test_set(test_size, random_state)

        return self

    def create_classifier(self, multiclass: bool) -> 'LightGBMClassifierBuilder':
        X, y, X_test, y_test = self._get_model()

        objective = 'multiclass' if multiclass else 'binary'

        self.classifier = LGBMClassifier(objective=objective, verbose=-1)
        self.classifier.fit(X, y)

        return self

    def get(self) -> LGBMClassifier:
        return self.classifier


class KNNClassifierBuilder(AbstractBuilder):
    classifier: KNeighborsClassifier

    def set_model(self, X, y) -> 'KNNClassifierBuilder':
        super().set_model(X, y)

        return self

    def create_train_test_set(self, test_size: float, random_state: int = 1) -> 'KNNClassifierBuilder':
        super().create_train_test_set(test_size, random_state)

        return self

    def create_classifier(self, n_neighbors: int, algorithm: str = None) -> 'KNNClassifierBuilder':
        X, y, X_test, y_test = self._get_model()
        algorithm = algorithm if algorithm is not None else 'auto'

        self.classifier = KNeighborsClassifier(n_neighbors=n_neighbors, algorithm=algorithm)
        self.classifier.fit(X, y)
        best_score = self.classifier.score(X_test, y_test)
        for i in range(1, n_neighbors):
            classifier = KNeighborsClassifier(n_neighbors=i, algorithm=algorithm)
            classifier.fit(X, y)
            if classifier.score(X_test, y_test) > best_score:
                self.classifier = classifier

        return self

    def get(self) -> KNeighborsClassifier:
        return self.classifier
