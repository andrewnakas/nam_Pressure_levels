#!/usr/bin/env python3
"""Clean up old forecast data to maintain rolling storage."""

import argparse
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import xarray as xr


def cleanup_old_forecasts(max_age_hours: int = 24, keep_latest_only: bool = True) -> None:
    """Remove forecast data older than the specified age.

    Args:
        max_age_hours: Maximum age of forecasts to keep in hours (ignored if keep_latest_only=True)
        keep_latest_only: If True, keep only the most recent forecast run(s)
    """
    data_dir = Path("data")

    # Process each Zarr dataset
    for zarr_path in data_dir.glob("*.zarr"):
        try:
            print(f"Processing {zarr_path}")

            # Open the dataset
            ds = xr.open_zarr(zarr_path, consolidated=True)

            # Get the append dimension (usually 'init_time')
            append_dim = "init_time"
            if append_dim not in ds.dims:
                print(f"Warning: {zarr_path} does not have '{append_dim}' dimension")
                ds.close()
                continue

            # Find indices to keep
            init_times = pd.DatetimeIndex(ds[append_dim].values)

            # Ensure timezone-naive for comparison
            if init_times.tz is not None:
                init_times = init_times.tz_localize(None)

            if keep_latest_only:
                # Keep only the most recent forecast run
                max_init_time = init_times.max()
                keep_mask = init_times == max_init_time
                print(f"Keeping only latest forecast: {max_init_time}")
            else:
                # Keep forecasts within the time window
                cutoff_time = pd.Timestamp.now(tz="UTC") - timedelta(hours=max_age_hours)
                cutoff_time_naive = cutoff_time.tz_localize(None)
                keep_mask = init_times >= cutoff_time_naive
                print(f"Keeping forecasts after: {cutoff_time_naive}")

            ds.close()

            # If we need to remove data
            if not keep_mask.all():
                num_to_remove = (~keep_mask).sum()
                num_to_keep = keep_mask.sum()
                print(f"Removing {num_to_remove} forecast runs, keeping {num_to_keep}")

                # Reopen in write mode and select only data to keep
                ds = xr.open_zarr(zarr_path, consolidated=True)
                ds_subset = ds.isel({append_dim: keep_mask})

                # Create temporary zarr store
                temp_path = zarr_path.parent / f"{zarr_path.name}.tmp"
                if temp_path.exists():
                    shutil.rmtree(temp_path)

                # Write subset to temporary store
                ds_subset.to_zarr(temp_path, mode="w", consolidated=True)
                ds.close()
                ds_subset.close()

                # Replace original with cleaned version
                shutil.rmtree(zarr_path)
                temp_path.rename(zarr_path)

                print(f"Cleaned up {zarr_path}")
            else:
                print(f"No cleanup needed for {zarr_path}")

        except Exception as e:
            print(f"Error processing {zarr_path}: {e}")
            continue


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean up old forecast data")
    parser.add_argument(
        "--max-age-hours",
        type=int,
        default=24,
        help="Maximum age of forecasts to keep in hours (ignored if --keep-latest-only)",
    )
    parser.add_argument(
        "--keep-latest-only",
        action="store_true",
        default=True,
        help="Keep only the most recent forecast run",
    )
    args = parser.parse_args()

    cleanup_old_forecasts(
        max_age_hours=args.max_age_hours,
        keep_latest_only=args.keep_latest_only,
    )
