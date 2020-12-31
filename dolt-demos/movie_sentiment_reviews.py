from metaflow import FlowSpec, step, DoltDT

from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from scipy.stats import uniform

from doltpy.core import Dolt
from doltpy.core.write import import_df
from doltpy.core.read import read_table

import re
import nltk
import pandas as pd

from nltk.corpus import stopwords

## Dolthub link: https://www.dolthub.com/repositories/vinai/imdb-reviews

class IMDBSentimentsFlow(FlowSpec):
    @step
    def start(self):
        with DoltDT(run=self, db_name='imdb-reviews') as dolt:
            self.train_table = dolt.get_table('reviews_train')
            self.test_table = dolt.get_table('reviews_test')
            
            # Split the train and test into matrices and labels
            self.train_reviews = self.train_table['review']
            self.train_labels = self.train_table['sentiment']

            self.test_reviews = self.test_table['review']
            self.test_labels = self.test_table['sentiment']

        self.next(self.bigram_representation)

    @step
    def bigram_representation(self):
        # Replace the new line markers in the reviews with spaces
        self.train_reviews.map(lambda txt: re.sub('(<br\s*/?>)+', ' ', txt))
        self.test_reviews.map(lambda txt: re.sub('(<br\s*/?>)+', ' ', txt))


        # Get the bigram representation of the data
        def bigram(reviews):
            bigram_vectorizer = CountVectorizer(ngram_range=(1, 2), stop_words=stopwords.words('english'))
            bigram_vectorizer.fit(reviews)
            vectorized = bigram_vectorizer.transform(reviews)

            bigram_tf_idf_transformer = TfidfTransformer()
            bigram_tf_idf_transformer.fit(vectorized)
            
            return bigram_vectorizer, bigram_tf_idf_transformer

        bv, bt = bigram(self.train_reviews)

        # Transform appropriately.
        def transform(bv, bt, reviews):
            r = bv.transform(reviews)
            return bt.transform(r)

        self.train_bigram, self.test_bigram = transform(bv, bt, self.train_reviews), transform(bv, bt, self.test_reviews)

        self.next(self.get_trained_model)

    @step
    def get_trained_model(self):
        # Create the classifier and search for the optimized parameters.
        clf = SGDClassifier()

        distributions = dict(
            penalty=['l1', 'l2', 'elasticnet'],
            alpha=uniform(loc=1e-6, scale=1e-4)
        )

        random_search_cv = RandomizedSearchCV(
            estimator=clf,
            param_distributions=distributions,
            cv=5,
            n_iter=3
        )

        random_search_cv.fit(self.train_bigram, self.train_labels)

        # Can pickle the model as well.
        self.model = random_search_cv.best_estimator_

        self.next(self.stats)

    @step
    def stats(self):
        train_score = self.model.score(self.train_bigram, self.train_labels)
        print("Train Score {}".format(round(train_score, 2)))

        train_predictions = self.model.predict(self.train_bigram)

        test_score = self.model.score(self.test_bigram, self.test_labels)
        print("Test Score {}".format(round(test_score, 2)))

        test_predictions = self.model.predict(self.test_bigram)

        # Output the predictions to a result table
        def push_prediction_to_table(reviews, labels, predictions, table_name):
            with DoltDT(run=self, db_name='imdb-reviews') as dolt:
                predictions = pd.Series(predictions).rename('predictions')

                result = pd.concat([reviews, labels, predictions], axis=1)
                dolt.add_table(table_name=table_name, df=result, pks=['review'])
        
        push_prediction_to_table(self.train_reviews, self.train_labels, train_predictions, "train_results")
        push_prediction_to_table(self.test_reviews, self.test_labels, test_predictions, "test_results")
        
        self.next(self.end)

    @step
    def end(self):
        # Commit and push. This can be in the previous step as well.
        with DoltDT(run=self, db_name='imdb-reviews') as dolt:
            dolt.commit_and_push()

 
if __name__ == '__main__':
    IMDBSentimentsFlow()
