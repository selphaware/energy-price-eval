import yaml
from src.CommodPriceEngine import CommodPriceEngine


def main():
    with open("../config/inputs.yaml") as file:
        cfg = yaml.load(file, Loader=yaml.FullLoader)

    cpe = CommodPriceEngine(params={
        "commod_ids": cfg['commod_ids'],
        "input_dir": cfg['input_dir'],
        "output_dir": cfg['output_dir'],
    })

    # Acquire EIA data and build price matrix per commodity id
    cpe.main()


if __name__ == "__main__":
    main()
