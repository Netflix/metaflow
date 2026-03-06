from baseflow import BaseParamsFlow
from metaflow import schedule


@schedule(cron="*/2 * * * *")
class CronParamFlow(BaseParamsFlow):
    pass


if __name__ == "__main__":
    CronParamFlow()
