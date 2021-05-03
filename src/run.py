import yaml
from src.CommodPriceEngine import CommodPriceEngine
from src.EvalForecastEngine import EvalForecastEngine
from src.logger import logger


def main() -> None:
    """
    Main function. RUN this from IDE directly OR via command "python run.py"

    :return: saves outputs data/output. evals in data/output/forecast_evals
    """
    logger.info("STARTED. Opening inputs.yaml")
    with open("../config/inputs.yaml") as file:
        cfg = yaml.load(file, Loader=yaml.FullLoader)

    # Instantiating pricing engine which builds the price matrix per commod id
    cpe = CommodPriceEngine(params={
        "commod_ids": cfg['commod_ids'].keys(),
        "input_dir": cfg['input_dir'],
        "output_dir": cfg['output_dir'],
    })

    # Instantiating evaluation forecast engine which evaluates all forecasts
    eve = EvalForecastEngine(params={
        "commod_ids": cfg['commod_ids'].keys(),
        "output_dir": cfg['output_dir'],
        "eval_params": cfg['eval_params']
    })

    # Acquire EIA data and build price matrix per commod id
    cpe.main(acquire=cfg['acquire'])

    # Evaluate all forecasts
    eve.eval_all_forecasts()


if __name__ == "__main__":
    main()
