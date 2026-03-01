# import json
# from pathlib import Path
# from .utils import ensure_dir, utc_timestamp


# def write_raw_json(dataset: str, payload: dict, base_path="/opt/airflow/data/bronze"):
#     """
#     Write a raw API response to a timestamped JSON file in the Bronze layer.

#     This function stores unmodified API payloads as raw JSON files, using a
#     timestamp-based filename to guarantee uniqueness and preserve ingestion
#     order. It is designed for use in ingestion pipelines where the Bronze
#     layer acts as the immutable landing zone for raw data.

#     Parameters:
#         dataset (str):
#             Name of the dataset being ingested (e.g., "games", "players").
#             A subfolder with this name will be created under the Bronze path.
#         payload (dict):
#             The JSON-serializable API response to write.
#         base_path (str, optional):
#             Root directory for Bronze storage. Defaults to
#             "/opt/airflow/data/bronze", which aligns with Airflow deployments.

#     Returns:
#         str:
#             Full file path of the written JSON file.

#     Notes:
#         - Filenames use Pacific Time timestamps for consistency across ingestion
#           tasks and alignment with your pipeline's time conventions.
#         - The function ensures the dataset directory exists before writing.
#         - Raw JSON is written with indentation for readability and debugging.
#     """
#     ts = utc_timestamp()
#     folder = f"{base_path}/{dataset}"
#     ensure_dir(folder)

#     file_path = f"{folder}/{ts}.json"
#     with open(file_path, "w") as f:
#         json.dump(payload, f, indent=2)

#     return file_path
