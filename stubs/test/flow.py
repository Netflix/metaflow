from metaflow import (
    FlowSpec,
    step,
    conda_base,
    Parameter,
    card,
    current,
    kubernetes,
    batch
)
from metaflow.cards import Markdown, Table, Image

BASE_URL = "https://metaflow-demo-public.s3.us-west-2.amazonaws.com/taxi/clean"
TRAIN_URL = BASE_URL + "/train_sample.parquet"
TEST_URL = BASE_URL + "/test.parquet"


@conda_base(
    libraries={
        "scikit-learn": "1.1.1",
        "pandas": "1.4.2",
        "pyarrow": "5.0.0",
        "matplotlib": "3.5.0",
    }
)
class FareRegressionFlow(FlowSpec):
    train_data_url = Parameter("train_url", default=TRAIN_URL)
    test_data_url = Parameter("test_url", default=TEST_URL)

    FEATURES = [
        "pickup_year",
        "pickup_dow",
        "pickup_hour",
        "abs_distance",
        "pickup_longitude",
        "dropoff_longitude",
    ]

    @card
    @step
    def start(self):
        import pandas as pd

        train_df = pd.read_parquet(self.train_data_url)
        self.X_train = train_df.loc[:, self.FEATURES]
        self.y_train = train_df.loc[:, "fare_amount"]
        print("Training set includes %d data points" % len(train_df))
        self.next(self.model)

    @card
    @kubernetes
    @step
    def model(self):
        self.rf_model = make_random_forest()
        self.rf_model.fit(self.X_train, self.y_train)
        self.next(self.evaluate)

    @card
    @batch
    @step
    def evaluate(self):
        from sklearn.metrics import mean_squared_error as mse
        import numpy as np
        import pandas as pd

        test_df = pd.read_parquet(self.test_data_url)
        self.X_test = test_df.loc[:, self.FEATURES]
        self.y_test = test_df.loc[:, "fare_amount"]
        n_rows = self.y_test.shape[0]

        self.y_pred = self.rf_model.predict(self.X_test)
        self.y_baseline_pred = np.repeat(self.y_test.mean(), n_rows)
        self.model_rmse = mse(self.y_test, self.y_pred)
        self.baseline_rmse = mse(self.y_test, self.y_baseline_pred)

        print(f"Model RMSE: {self.model_rmse}")
        print(f"Baseline RMSE: {self.baseline_rmse}")
        self.next(self.create_report)

    @card(type="blank")
    @step
    def create_report(self):
        self.plot = plot(self.y_test, self.y_pred)
        current.card.append(Markdown("# Model Report"))
        current.card.append(
            Table(
                [
                    ["Random Forest", float(self.model_rmse)],
                    ["Baseline", float(self.baseline_rmse)],
                ],
                headers=["Model", "RMSE"],
            )
        )
        current.card.append(Image(self.plot, label="Correct vs. Predicted Fare"))
        self.next(self.end)

    @step
    def end(self):
        pass


def make_random_forest():
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.compose import ColumnTransformer

    ct_pipe = ColumnTransformer(
        transformers=[
            ("encoder", OneHotEncoder(categories="auto", sparse=False), ["pickup_dow"]),
            (
                "std_scaler",
                StandardScaler(),
                ["abs_distance", "pickup_longitude", "dropoff_longitude"],
            ),
        ]
    )
    return Pipeline(
        [
            ("ct", ct_pipe),
            (
                "forest_reg",
                RandomForestRegressor(
                    n_estimators=10, n_jobs=-1, random_state=3, max_features=8
                ),
            ),
        ]
    )


def plot(correct, predicted):
    import matplotlib.pyplot as plt
    from io import BytesIO
    import numpy

    MAX_FARE = 100
    line = numpy.arange(0, MAX_FARE, MAX_FARE / 1000)
    plt.rcParams.update({"font.size": 22})
    plt.scatter(x=correct, y=predicted, alpha=0.01, linewidth=0.5)
    plt.plot(line, line, linewidth=2, color="black")
    plt.xlabel("Correct fare")
    plt.ylabel("Predicted fare")
    plt.xlim([0, MAX_FARE])
    plt.ylim([0, MAX_FARE])
    fig = plt.gcf()
    fig.set_size_inches(18, 10)
    buf = BytesIO()
    fig.savefig(buf)
    return buf.getvalue()


if __name__ == "__main__":
    FareRegressionFlow()
