#!/usr/bin/env python3
"""Create a summary document of available datasets."""

from datetime import datetime
from pathlib import Path

import xarray as xr


def format_size(size_bytes: int) -> str:
    """Format size in bytes to human-readable format."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def get_directory_size(path: Path) -> int:
    """Calculate total size of directory."""
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            total += item.stat().st_size
    return total


def create_summary() -> None:
    """Create a summary document of available datasets."""
    data_dir = Path("data")
    summary_dir = Path("data_summary")
    summary_dir.mkdir(parents=True, exist_ok=True)

    summary_lines = [
        "# NAM Pressure Levels Data Summary\n",
        f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n",
        "\n## Available Datasets\n",
    ]

    # Process each Zarr dataset
    for zarr_path in sorted(data_dir.glob("*.zarr")):
        try:
            ds = xr.open_zarr(zarr_path, consolidated=True)

            # Get basic info
            dataset_id = zarr_path.stem
            size = get_directory_size(zarr_path)

            summary_lines.append(f"\n### {dataset_id}\n")
            summary_lines.append(f"\n**Storage Size:** {format_size(size)}\n")

            # Add metadata
            if "title" in ds.attrs:
                summary_lines.append(f"\n**Title:** {ds.attrs['title']}\n")
            if "description" in ds.attrs:
                summary_lines.append(f"\n**Description:** {ds.attrs['description']}\n")

            # Dimensions
            summary_lines.append("\n**Dimensions:**\n")
            for dim, size in ds.dims.items():
                summary_lines.append(f"- {dim}: {size}\n")

            # Variables
            summary_lines.append("\n**Variables:**\n")
            for var in ds.data_vars:
                summary_lines.append(f"- {var}\n")

            # Time range
            if "init_time" in ds.dims:
                init_times = ds.init_time.values
                summary_lines.append(
                    f"\n**Latest Forecast:** {init_times[-1]}\n"
                )

            ds.close()

        except Exception as e:
            print(f"Warning: Could not process {zarr_path}: {e}")
            continue

    # Write summary
    summary_path = summary_dir / "summary.md"
    with open(summary_path, "w") as f:
        f.writelines(summary_lines)

    print(f"Summary written to {summary_path}")


if __name__ == "__main__":
    create_summary()
