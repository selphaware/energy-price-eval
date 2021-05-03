import os
import pandas as pd
from pandas import melt
import calendar
from datetime import datetime
from src.logger import logger

"""

    class: CommodPriceEngine
    
    This class extracts raw prices from EIA files, transforms,
    and pivots (rows = ref_date, cols = hist/forecast dates, vals =
    price OR percentage change in price).
    
    Data is saved in output folder:
    - data/output/raw_prices (raw csv prices)
    - data/output/transform (intermediate)
    - data/output/matrix_price (price matrix for each commod id)
        
"""


class CommodPriceEngine(object):
    def __init__(self, **params):
        """
        constructor initialising list of commod ids and input/output dirs

        :param params:
        """
        logger.info(f"Initialising CommodPriceEngine. Params = {str(params)}")
        params = params.get('params')
        self.commod_ids = params.get('commod_ids')
        self.input_dir = params.get('input_dir')
        self.output_dir = params.get('output_dir')
        self.raw_prices_dir = os.path.join(self.output_dir, "raw_prices")
        self.transform_dir = os.path.join(self.output_dir, "transform")
        self.transform_file = os.path.join(self.transform_dir,
                                           "full_transform.csv")
        self.matrix_price_dir = os.path.join(self.output_dir, "matrix_price")

    def extract_raw_prices(self, input_sheets: dict = None) -> None:
        """
        Extracts raw prices from EIA xls and xlsx files

        :param input_sheets: sheet names and headers to extract from
        :return: saves raw files in data/raw_prices
        """
        logger.info("Extracting raw pricing data")
        data_folder = self.input_dir

        # sheet names and headers to extract from
        # NOTE: Only 1 of these sheets will exist in the input spreadsheet
        input_sheets = {
            "2tab": ("new_format", [2, 3]),
            " Prices US": ("old_format", [2]),
            "All Prices": ("old_format", [2])
        } if input_sheets is None else input_sheets

        # loop through EIA files in input data folder and extract raw prices
        eia_files = next(os.walk(data_folder))[2]
        for eia_file in eia_files:
            # skip files already open or non-xls or xlsx files
            if ("~" == eia_file[0]) or (
                    "xls" not in eia_file.split(".")[1].lower()):
                continue

            # open excel file and select existing sheet with energy prices
            xl = pd.ExcelFile(os.path.join(data_folder, eia_file))
            sheet_name = None
            for s_name in input_sheets.keys():
                if s_name in xl.sheet_names:
                    sheet_name = s_name
                    break

            # if sheet doesn't exist, skip
            if sheet_name is None:
                continue

            # read prices sheet
            format_type, header_info = input_sheets[sheet_name]
            df = xl.parse(sheet_name, header=header_info)

            # extract reference date (i.e. when report was published)
            ref_date = eia_file.split(".")[0]
            ref_date = ref_date.split("_")[0]

            # extract month and year
            mth = ref_date[0:3]
            yr = "20" + ref_date[3:]  # pre: input files will be in this century

            # set ref_date and save raw prices as csv
            df[("ref_date", "0")] = f"{yr}-{mth}"
            df[("ref_date", "0")] = pd.to_datetime(df[("ref_date", "0")])
            df.to_csv(
                os.path.join(
                    self.raw_prices_dir,
                    "{}-{}.csv".format(ref_date, format_type)
                ), index=False)

    @staticmethod
    def convert_date_fmt(inp_date: str) -> str:
        """
        static function to convert input string date to format YYYY-MMM

        :param inp_date: input date string
        :return: str format YYYY-MMM (3 letter month name)
        """
        # format date to YYYY-MM
        res = datetime.strptime(inp_date, "%Y%m").strftime("%Y-%m")

        # build dic of month int : month name (3-letter)
        cal_dic_idx = {
            index: month for index, month in enumerate(
                calendar.month_abbr
            ) if month
        }

        # extract year and month
        yr = res.split("-")[0]
        mn = cal_dic_idx[int(res.split("-")[1])]

        # return formatted date of YYYY-MMM
        return f"{yr}-{mn}"

    def transform_prices(self) -> None:
        """
        loops through all files in raw_prices dir and transforms to:
        commod_id, ref_date, (historical/forecast) date, price

        :return: saves full transform file of
                 all commod ids in data/output/transform
        """
        logger.info("Transforming raw pricing data")

        # append results to ret list
        ret = []

        # loop through raw_prices dir transforming to:
        # commod_id, ref_date, (historical/forecast) date, price
        raw_folder = self.raw_prices_dir
        raw_files = next(os.walk(raw_folder))[2]
        for raw_file in raw_files:
            # if file not csv, skip
            if "csv" not in raw_file.split(".")[1].lower():
                continue

            # check if file is of old format or new and set header accordingly
            old_format = "old_format" in raw_file
            header = [0] if old_format else [0, 1]

            # read raw prices file and filter on commod_id's of interest
            df = pd.read_csv(os.path.join(raw_folder, raw_file), header=header)
            df = df[df[df.columns[0]].isin(self.commod_ids)]

            # set ref_date indexing (nuance for old format is applied)
            ref_date_idx = "('ref_date', '0')" if old_format else ("ref_date",
                                                                   "0")
            df.set_index([df.columns[0], ref_date_idx], inplace=True)
            df.drop(list(df.columns[0:1]), inplace=True, axis=1)

            # transpose df and format dates depending on new or old format
            df = df.T
            df["date"] = df.index
            date_cnvt_func = self.convert_date_fmt if old_format \
                else lambda x: f"{x[0]}-{x[1]}"
            df["date"] = df["date"].apply(date_cnvt_func)
            res = melt(df, id_vars=["date"])

            # final prep and casting on dataframe
            res.columns = ["date", "commodity_id", "ref_date", "price"]
            res["date"] = pd.to_datetime(res["date"])
            res["price"] = res["price"].astype(float)

            # append dataframe to ret list, which will concat'd at the end
            ret.append(res)

        # concat all transformed dataframes into one dataframe, then save
        tdf = pd.concat(ret)
        tdf.reset_index(inplace=True, drop=True)
        tdf.to_csv(
            self.transform_file,
            index=False
        )

    def build_single_price_matrix(self, commod_id: str) -> None:
        """
        picks up transformed file for a commod id and builds price
        matrix of: rows = reference dates (date of when file is published),
        columns = historical / forecast dates.
        values = prices OR percentage diff

        :param commod_id: e.g. WTIPUUS
        :return: saves price matrix for a commod id in data/output/matrix_price
        """
        logger.info(f"{commod_id}: Building price and pct diff matrix")

        # read full transform file and filter on specific commod id
        tdf = pd.read_csv(self.transform_file)
        prev_df = tdf[tdf["commodity_id"] == commod_id].copy().sort_values(
            by=["ref_date", "date"], ascending=True
        )

        # calculate percentage differences on price
        ret = []
        for in_ref_date in sorted(tdf["ref_date"].unique()):
            in_df = prev_df[prev_df["ref_date"] == in_ref_date].copy()
            in_df["pct_diff"] = in_df["price"].pct_change()
            ret.append(in_df)
        df = pd.concat(ret).sort_values(by=["ref_date", "date"], ascending=True)
        df.drop(columns=["commodity_id"], inplace=True)

        # pivot & save. rows = ref_date, cols = date, vals = price
        piv_df = df.pivot_table(
            index=['ref_date'],
            columns=['date'],
            values='price'
        )
        piv_df.to_csv(
            os.path.join(self.matrix_price_dir,
                         "{}_price.csv".format(commod_id))
        )

        # pivot & save. rows = ref_date, cols = date, vals = percentage diff
        piv_df = df.pivot_table(
            index=['ref_date'],
            columns=['date'],
            values='pct_diff'
        )
        piv_df.to_csv(
            os.path.join(self.matrix_price_dir, "{}_pct.csv".format(commod_id))
        )

    def build_price_matrix(self) -> None:
        """
        build all price matrix for all commod id's

        :return: saves all price matrix outputs to data/output/matrix_price
        """
        logger.info("Building pricing matrix for ALL Commod ID's")
        for commod_id in self.commod_ids:
            self.build_single_price_matrix(commod_id)

    def main(self, acquire=True) -> None:
        """
        run all functions in order: extract, transform, price matrix

        :param acquire: False = read already extracted raw csv's
                        True = extract prices from EIA files into raw csv's
        :return: saves outputs in data/output
        """
        logger.info("CommodPriceEngine MAIN started")
        if acquire:
            self.extract_raw_prices()
        self.transform_prices()
        self.build_price_matrix()
