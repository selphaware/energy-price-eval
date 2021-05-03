import yaml
from src.CommodPriceEngine import CommodPriceEngine
from src.EvalForecastEngine import EvalForecastEngine
from src.logger import logger


def main():
    logger.info("STARTED. Opening inputs.yaml")
    with open("../config/inputs.yaml") as file:
        cfg = yaml.load(file, Loader=yaml.FullLoader)

    logger.info("Instantiating pricing and forecast-evaluation engines")
    cpe = CommodPriceEngine(params={
        "commod_ids": cfg['commod_ids'],
        "input_dir": cfg['input_dir'],
        "output_dir": cfg['output_dir'],
    })

    eve = EvalForecastEngine(params={
        "commod_ids": cfg['commod_ids'],
        "output_dir": cfg['output_dir'],
        "eval_params": cfg['eval_params']
    })

    logger.info("Starting to build Price Matrix")
    # Acquire EIA data and build price matrix per commodity id
    cpe.main(acquire=cfg['acquire'])

    logger.info("Starting to evaluate all forecasts")
    # Evaluate all forecasts
    eve.eval_all_forecasts()


if __name__ == "__main__":
    main()
