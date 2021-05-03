import os
import pandas as pd
from itertools import product
from datetime import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
from src.logger import logger
from sklearn.metrics import mean_absolute_error as mae
from sklearn.metrics import mean_squared_error as mse
from sklearn.metrics import max_error as max_error

"""

    class: EvalForecastEngine

    This class evaluates the forecasts in the EIA files via 
    the price matrix in data/output/matrix_price for each commod id.

    Data is saved in data/output/forecast_evals folder.

"""


class EvalForecastEngine(object):
    def __init__(self, **params):
        """
        constructor intialising list of commod ids and input/output dirs.
        additionally, setting universe of evaluation functions

        :param params:
        """
        logger.info(f"Initialising EvalForecastEngine. Params = {str(params)}")
        params = params.get('params')
        self.commod_ids = params.get('commod_ids')
        self.output_dir = params.get('output_dir')
        self.matrix_price_dir = os.path.join(self.output_dir, "matrix_price")
        self.eval_dir = os.path.join(self.output_dir, "forecast_evals")
        self.metrics = params.get('eval_params')['metrics']
        self.time_horizons = params.get('eval_params')['time_horizons']

        # forecast evaluation funcs to be called dynamically in eval_forecasts
        # NOTE: metrics to be run are in self.metrics
        self.eval_funcs = {
            "mae": mae,             # MEAN ABSOLUTE ERROR
            "mse": mse,             # MEAN SQUARED ERROR
            "rmse": mse,            # ROOT MEAN SQUARED ERROR
            "max_error": max_error  # MAX ERROR
        }

    def eval_forecasts(self, commod_id: str, dat_type: str) -> None:
        """
        reads price matrix for this commod id and evaluates all forecasts

        :param commod_id: evaluate forecasts for this commod id e.g. WTIPUUS
        :param dat_type: price or pct (price percentage differences)
        :return:
        """
        logger.info(f"{commod_id} [{dat_type}]: Evaluating all forecasts")

        # read price matrix for commod_id
        price_matrix = pd.read_csv(
            os.path.join(self.matrix_price_dir, f"{commod_id}_{dat_type}.csv"),
            parse_dates=["ref_date"],
            date_parser=lambda x: datetime.strptime(x, '%Y-%m-%d')
        )

        # initialise frame of time horizons and metrics.
        # e.g. 1M_MAE, 6M_RMSE, etc. this will store forecasts for all ref_date
        ret = {f"{x}M_{y}": [] for x, y in product(self.time_horizons,
                                                   self.metrics)}

        # go through all references dates (ref_date) and evaluate all price
        # and pct diff forecasts for all eval metrics and time horizons
        ret["ref_date"] = []
        for ix in range(price_matrix.shape[0]):
            # get current reference date
            idx = price_matrix.iloc[ix, 0]
            ref_date = price_matrix.loc[
                price_matrix["ref_date"] == idx, price_matrix.columns[0]
            ].reset_index(drop=True)[0]
            ret["ref_date"].append(ref_date)

            # filter on prices for this ref_date
            sub_matrix = price_matrix.loc[
                price_matrix["ref_date"] == idx, price_matrix.columns[1:]]
            sub_matrix.columns = pd.to_datetime(sub_matrix.columns)

            # loop through each time horizon and eval metric, computing
            # the evaluations for this reference date (ref_date)
            sub_matrix = sub_matrix.T
            for time_h, metric in product(self.time_horizons, self.metrics):
                # extract forecast data
                end_date = ref_date + relativedelta(months=time_h)
                fore_dat = sub_matrix[
                    (sub_matrix.index >= ref_date) &
                    (sub_matrix.index < end_date)
                ]

                # extract true values data
                sub_true = price_matrix.loc[
                    price_matrix["ref_date"] == idx + relativedelta(
                        months=time_h
                    ), price_matrix.columns[1:]
                ]
                sub_true.columns = pd.to_datetime(sub_true.columns)
                sub_true = sub_true.T
                true_dat = sub_true[
                    (sub_true.index >= ref_date) &
                    (sub_true.index < end_date)
                ]

                # evaluate forecasts and append results in ret
                if true_dat.shape[1]:
                    if metric == "rmse":
                        ret[f"{time_h}M_{metric}"].append(
                            self.eval_funcs[metric](np.array(fore_dat),
                                                    np.array(true_dat),
                                                    squared=False))
                    else:
                        ret[f"{time_h}M_{metric}"].append(
                            self.eval_funcs[metric](np.array(fore_dat),
                                                    np.array(true_dat)))
                else:
                    ret[f"{time_h}M_{metric}"].append(None)

        # save forecast evaluations for all ref_date's
        df = pd.DataFrame(ret)
        df['commod_id'] = commod_id
        df.reset_index(inplace=True, drop=True)
        df = df[["commod_id", "ref_date"] + [f"{x}M_{y}" for x, y in
                                             product(self.time_horizons,
                                                     self.metrics)]]
        df.to_csv(os.path.join(self.eval_dir, f"{commod_id}_{dat_type}.csv"),
                  index=False)

    def eval_all_forecasts(self) -> None:
        """
        evaluate all forecasts for all commod ids

        :return: saves evaluations in data/output/forecast_evals
        """
        logger.info("Evaluating ALL Commod ID's Forecasts")
        for commod_id in self.commod_ids:
            self.eval_forecasts(commod_id, "price")
            self.eval_forecasts(commod_id, "pct")  # price percentage diff
