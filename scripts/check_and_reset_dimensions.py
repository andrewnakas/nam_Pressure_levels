#!/usr/bin/env python3
"""Check and reset Zarr dimensions if needed."""

import shutil
from pathlib import Path

import xarray as xr


def check_and_reset_dimensions() -> None:
    """Check if Zarr dimensions are corrupted and reset if needed."""
    data_dir = Path("data")

    for zarr_path in data_dir.glob("*.zarr"):
        try:
            # Try to open the dataset
            ds = xr.open_zarr(zarr_path, consolidated=True)

            # Check for dimension issues
            if "init_time" in ds.dims:
                init_time_size = ds.dims["init_time"]
                actual_size = len(ds.init_time.values)

                if init_time_size != actual_size:
                    print(f"Dimension mismatch in {zarr_path}: {init_time_size} != {actual_size}")
                    print("Resetting dataset...")
                    ds.close()

                    # Backup and remove
                    backup_path = zarr_path.parent / f"{zarr_path.name}.backup"
                    if backup_path.exists():
                        shutil.rmtree(backup_path)
                    shutil.move(zarr_path, backup_path)
                    print(f"Backed up to {backup_path}")
                else:
                    print(f"{zarr_path} dimensions OK")
                    ds.close()
            else:
                print(f"{zarr_path} has no init_time dimension")
                ds.close()

        except Exception as e:
            print(f"Error checking {zarr_path}: {e}")
            print("Dataset may be corrupted, removing...")
            if zarr_path.exists():
                shutil.rmtree(zarr_path)


if __name__ == "__main__":
    check_and_reset_dimensions()
