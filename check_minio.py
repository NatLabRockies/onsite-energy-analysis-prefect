from onsite_prefect.config import SizingStrategy, Technology
from onsite_prefect.minio import filter_existing_match_ids

MATCH_ID = "MNc7Kfu4"
TECHNOLOGY = Technology.wind
OPTION = SizingStrategy.A


def main() -> None:
    result = filter_existing_match_ids(
        TECHNOLOGY,
        OPTION,
        [MATCH_ID],
        overwrite_existing_results=False,
    )
    exists = MATCH_ID in result.existing_match_ids
    print(f"{MATCH_ID} exists in MinIO: {exists}")


if __name__ == "__main__":
    main()
