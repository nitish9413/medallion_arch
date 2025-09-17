from prefect import task, flow, get_run_logger
from pathlib import Path
import polars as pl

LAYER_PATH = "./bronze"


@task
def initialize_delta_table(layer_path: str = "./bronze"):
    logger = get_run_logger()
    path = Path(layer_path)
    path.mkdir(exist_ok=True)
    if not (path / "_delta_log").exists():
        logger.info("Delta table missing, initializing with empty DataFrame...")
        df = pl.DataFrame(
            {
                "Open_time": pl.Series(name="Open_time", values=[], dtype=pl.Int64),
                "Open": pl.Series(name="Open", values=[], dtype=pl.Float64),
                "High": pl.Series(name="High", values=[], dtype=pl.Float64),
                "Low": pl.Series(name="Low", values=[], dtype=pl.Float64),
                "Close": pl.Series(name="Close", values=[], dtype=pl.Float64),
                "Volume": pl.Series(name="Volume", values=[], dtype=pl.Float64),
                "Close_time": pl.Series(name="Close_time", values=[], dtype=pl.Int64),
                "Quote_asset_volume": pl.Series(
                    name="Quote_asset_volume", values=[], dtype=pl.Float64
                ),
                "Number_of_trades": pl.Series(
                    name="Number_of_trades", values=[], dtype=pl.Int64
                ),
                "Taker_buy_base_asset_volume": pl.Series(
                    name="Taker_buy_base_asset_volume", values=[], dtype=pl.Float64
                ),
                "Taker_buy_quote_asset_volume": pl.Series(
                    name="Taker_buy_quote_asset_volume", values=[], dtype=pl.Float64
                ),
                "Ignore": pl.Series(name="Ignore", values=[], dtype=pl.Float64),
            }
        )

        df.write_delta(target=layer_path, mode="overwrite")
        logger.info("Delta table initialized.")


@task
def get_data_files(target_path: str = "./raw") -> list[Path]:
    logger = get_run_logger()
    files = list(Path(target_path).glob("*.csv"))
    logger.info(f"Found {len(files)} CSV files.")
    return files


@task
def drop_in_bronze_layer(file_path: Path, layer_path: str = LAYER_PATH) -> bool:
    logger = get_run_logger()
    logger.info(f"Reading file: {file_path}")
    try:
        df = pl.read_csv(file_path, has_header=False)
        df.columns = [
            "Open_time",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "Close_time",
            "Quote_asset_volume",
            "Number_of_trades",
            "Taker_buy_base_asset_volume",
            "Taker_buy_quote_asset_volume",
            "Ignore",
        ]
        Path(layer_path).mkdir(exist_ok=True)

        if not (Path(layer_path) / "_delta_log").exists():
            logger.info("Delta table missing, initializing...")
            df.head(0).write_delta(target=layer_path, mode="overwrite")

        (
            df.write_delta(
                target=layer_path,
                mode="merge",
                delta_merge_options={
                    "predicate": "s.Open_time = t.Open_time",
                    "source_alias": "s",
                    "target_alias": "t",
                },
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )

        logger.info(f"{file_path.name} saved successfully.")
        return True
    except Exception as e:
        logger.error(f"Error processing {file_path.name}: {e}")
        return False


@flow
def bronze_layer():
    logger = get_run_logger()
    logger.info("Starting the bronze layer flow.")
    initialize_delta_table()
    files_list = get_data_files()
    # drop_in_bronze_layer.map(files_list)
    for file in files_list:
        drop_in_bronze_layer(file)
    logger.info("Finished processing files.")


if __name__ == "__main__":
    bronze_layer()
