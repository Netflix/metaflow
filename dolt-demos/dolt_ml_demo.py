from metaflow import FlowSpec, step, DoltDT
import pandas as pd
import pickle
from sklearn import tree

class DoltMLDemoFlow(FlowSpec):
    @step
    def start(self):
        # Start by getting original dataset
        with DoltDT(run=self, db_name='iris-test') as dolt:
            self.test_set = dolt.get_table('iris-test')

        self.next(self.predict)

    @step
    def predict(self):
        with DoltDT(run=self, db_name='iris-model-results') as dolt:
            self.model = pickle.load(open('model.p', 'rb'))
            self.model_type = 'Decision Tree'

            samples = self.test_set['sample']
            y_true = self.test_set['species']
            y_true = y_true.rename('labels')

            test = self.test_set.drop(columns=['species', 'sample'])
            predictions = pd.Series(self.model.predict(test))
            predictions = predictions.rename('predictions')

            self.result = pd.concat([samples, y_true, predictions], axis=1)

            dolt.add_table(table_name='result', df=self.result, pks=['sample'])

        self.next(self.end)

    @step
    def end(self):
        with DoltDT(run=self, db_name='iris-model-results') as dolt:
            dolt.commit_table_writes()


if __name__ == '__main__':
    DoltMLDemoFlow()
