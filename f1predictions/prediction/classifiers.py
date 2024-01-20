import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LinearRegression
import sys
from sklearn.tree import DecisionTreeClassifier
from f1predictions.prediction import AbstractBuilder, AbstractDecisionTreeBasedClassifierBuilder


class RandomForestClassifierBuilder(AbstractDecisionTreeBasedClassifierBuilder):
    Classifier: RandomForestClassifier

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
    Classifier: DecisionTreeClassifier

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
            Classifier = DecisionTreeClassifier(criterion=self.criterion, max_depth=i)

            Classifier.fit(X, y)
            score = Classifier.score(X_test, y_test)
            if score > best_score:
                best_classifier, best_score = Classifier, score

        self.classifier = best_classifier

        return self

    def get(self) -> DecisionTreeClassifier:
        return self.classifier


class LogisticRegressionClassifierBuilder(AbstractBuilder):
    Classifier: LinearRegression

    def set_model(self, X: np.array, y: np.array) -> 'LogisticRegressionClassifierBuilder':
        self.X = X
        self.y = y

        return self

    def create_train_test_set(self, test_size: float, random_state: int = 1) -> 'LogisticRegressionClassifierBuilder':
        super().create_train_test_set(test_size, random_state)

        return self

    def create_classifier(self) -> 'LogisticRegressionClassifierBuilder':
        X, y, X_test, y_test = self._get_model()
        self.classifier = LinearRegression()
        self.classifier.fit(X, y)

        return self

    def get(self) -> LinearRegression:
        return self.classifier