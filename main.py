import asyncio

from onsite_prefect import Config, Range, SiteID, SizingStrategy, Technology, run_scenario

__all__ = [
    "Config",
    "Range",
    "SiteID",
    "SizingStrategy",
    "Technology",
    "run_scenario",
]


if __name__ == "__main__":
    async def main():
        await run_scenario(
            Config(
                technology=Technology.wind,
                sizing_strategy=SizingStrategy.A,
                overwrite_existing_results=True,
                # sites=Range(),
                sites=SiteID(site_ids=["MNc7Kfu4"]),
            ),
        )

    asyncio.run(main())
