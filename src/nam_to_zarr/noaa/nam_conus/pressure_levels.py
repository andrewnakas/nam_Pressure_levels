"""NAM CONUS pressure levels dataset."""

from __future__ import annotations

import logging
import re
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests
import xarray as xr
import zarr
from numcodecs import Zstd

logger = logging.getLogger(__name__)


@dataclass
class DatasetAttributes:
    """Dataset metadata attributes."""

    title: str
    description: str
    provider: str
    model: str
    variant: str
    version: str
    source: str
    references: str


@dataclass
class TemplateConfig:
    """Configuration for dataset template."""

    dataset_attributes: DatasetAttributes


class NamConusPressureLevelsDataset:
    """NAM CONUS pressure levels dataset handler."""

    def __init__(self) -> None:
        """Initialize the dataset handler."""
        self.template_config = TemplateConfig(
            dataset_attributes=DatasetAttributes(
                title="NOAA NAM CONUS Pressure Levels Forecast",
                description="North American Mesoscale (NAM) CONUS pressure level forecast data at 12km resolution",
                provider="NOAA/NCEP",
                model="NAM",
                variant="CONUS-12km-pressure-levels",
                version="v1.0",
                source="https://nomads.ncep.noaa.gov",
                references="https://www.nco.ncep.noaa.gov/pmb/products/nam/",
            )
        )

        # NAM CONUS specifications
        self.grid_resolution = 12.19  # km
        self.grid_shape = (428, 614)  # y, x
        self.forecast_hours = list(range(0, 49))  # 0-48 hours (reduced to fit under 2GB GitHub limit)
        self.cycles = ["00", "06", "12", "18"]  # 4 times daily

        # Pressure levels in hPa
        self.pressure_levels = [
            1000, 975, 950, 925, 900, 850, 800, 750, 700,
            650, 600, 550, 500, 450, 400, 350, 300, 250,
            200, 150, 100
        ]

        # Key variables for pressure levels
        self.variables = {
            "TMP": "Temperature",
            "RH": "Relative Humidity",
            "UGRD": "U-component of Wind",
            "VGRD": "V-component of Wind",
            "HGT": "Geopotential Height",
            "VVEL": "Vertical Velocity",
            "ABSV": "Absolute Vorticity",
        }

    def get_latest_cycle(self) -> tuple[datetime, str]:
        """Get the latest available NAM cycle.

        Returns:
            Tuple of (cycle datetime, cycle hour string)
        """
        now = datetime.now(timezone.utc)

        # NAM cycles are at 00, 06, 12, 18 UTC
        # Data is typically available 3-4 hours after cycle time
        cycle_hours = [0, 6, 12, 18]

        # Find the most recent cycle that should be available
        for hours_ago in range(4, 24):  # Check up to 24 hours back
            check_time = now - timedelta(hours=hours_ago)
            cycle_hour = max([h for h in cycle_hours if h <= check_time.hour])
            cycle_time = check_time.replace(hour=cycle_hour, minute=0, second=0, microsecond=0)

            # Check if this cycle exists
            if self._check_cycle_exists(cycle_time):
                cycle_str = f"{cycle_hour:02d}"
                logger.info(f"Found latest cycle: {cycle_time.strftime('%Y%m%d')} {cycle_str}Z")
                return cycle_time, cycle_str

        msg = "Could not find available NAM cycle"
        raise RuntimeError(msg)

    def _check_cycle_exists(self, cycle_time: datetime) -> bool:
        """Check if a NAM cycle exists on NOMADS."""
        date_str = cycle_time.strftime("%Y%m%d")
        cycle_hour = f"{cycle_time.hour:02d}"

        # Try to access the directory listing
        url = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod/nam.{date_str}/"

        try:
            response = requests.get(url, timeout=30)
            return response.status_code == 200
        except Exception:
            return False

    def download_grib_file(
        self,
        cycle_time: datetime,
        cycle_hour: str,
        forecast_hour: int,
        output_path: Path,
    ) -> Path:
        """Download a single NAM GRIB2 file using grib filter.

        Args:
            cycle_time: Cycle initialization time
            cycle_hour: Cycle hour string (e.g., "00", "06")
            forecast_hour: Forecast hour
            output_path: Path to save the GRIB file

        Returns:
            Path to downloaded file
        """
        date_str = cycle_time.strftime("%Y%m%d")

        # Build grib filter URL
        base_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_nam.pl"

        # File name pattern for NAM pressure level files
        file_name = f"nam.t{cycle_hour}z.awphys{forecast_hour:02d}.tm00.grib2"

        # Build filter parameters
        params = {
            "file": file_name,
            "dir": f"/nam.{date_str}",
        }

        # Add all pressure levels
        for level in self.pressure_levels:
            params[f"lev_{level}_mb"] = "on"

        # Add all variables
        for var in self.variables:
            params[f"var_{var}"] = "on"

        # Add subregion (full CONUS grid)
        params["subregion"] = ""
        params["leftlon"] = "0"
        params["rightlon"] = "360"
        params["toplat"] = "90"
        params["bottomlat"] = "-90"

        logger.info(f"Downloading forecast hour {forecast_hour} from cycle {date_str} {cycle_hour}Z")

        try:
            response = requests.get(base_url, params=params, timeout=300)
            response.raise_for_status()

            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(response.content)

            logger.info(f"Downloaded {output_path.stat().st_size / 1024 / 1024:.1f} MB")
            return output_path

        except Exception as e:
            logger.error(f"Failed to download forecast hour {forecast_hour}: {e}")
            raise

    def grib_to_zarr(
        self,
        grib_files: list[Path],
        output_dir: Path,
        append: bool = False,
    ) -> Path:
        """Convert GRIB files to Zarr format.

        Args:
            grib_files: List of GRIB file paths
            output_dir: Output directory for Zarr store
            append: Whether to append to existing Zarr store

        Returns:
            Path to Zarr store
        """
        zarr_path = output_dir / "nam_conus_pressure_levels.zarr"

        logger.info(f"Converting {len(grib_files)} GRIB files to Zarr")

        # Open all GRIB files
        datasets = []
        for grib_file in sorted(grib_files):
            try:
                ds = xr.open_dataset(
                    grib_file,
                    engine="cfgrib",
                    backend_kwargs={
                        "filter_by_keys": {"typeOfLevel": "isobaricInhPa"},
                        "indexpath": "",
                    },
                )
                datasets.append(ds)
            except Exception as e:
                logger.warning(f"Failed to open {grib_file}: {e}")
                continue

        if not datasets:
            msg = "No valid GRIB files to process"
            raise RuntimeError(msg)

        # Concatenate along time dimension
        combined = xr.concat(datasets, dim="time")

        # Add init_time dimension for appending
        init_time = combined.time.values[0]
        combined = combined.expand_dims({"init_time": [init_time]})

        # Rename dimensions for consistency
        if "latitude" in combined.dims:
            combined = combined.rename({"latitude": "y", "longitude": "x"})
        if "isobaricInhPa" in combined.dims:
            combined = combined.rename({"isobaricInhPa": "level"})

        # Add dataset attributes
        attrs = self.template_config.dataset_attributes
        combined.attrs.update({
            "title": attrs.title,
            "description": attrs.description,
            "provider": attrs.provider,
            "model": attrs.model,
            "variant": attrs.variant,
            "version": attrs.version,
            "source": attrs.source,
            "references": attrs.references,
            "created": datetime.now(timezone.utc).isoformat(),
        })

        # Write to Zarr
        if append and zarr_path.exists():
            logger.info("Appending to existing Zarr store")
            # Don't provide encoding when appending - it will use existing encoding
            combined.to_zarr(
                zarr_path,
                mode="a",
                append_dim="init_time",
                consolidated=True,
            )
        else:
            logger.info(f"Creating new Zarr store at {zarr_path}")
            # Configure chunking and compression only for new stores
            # Chunk to stay under GitHub's 100MB file limit
            # Split spatially into 3x3 grid = 9 chunks per variable
            encoding = {}
            for var in combined.data_vars:
                encoding[var] = {
                    "compressor": Zstd(level=3),
                    # Chunks: (1 init, all times, all levels, 1/3 y, 1/3 x)
                    # = 9 files per variable (7 vars * 9 = 63 files total)
                    # Each file ~40-60MB (safely under 100MB limit)
                    "chunks": (1, combined.dims["time"], len(self.pressure_levels), 143, 205),
                }
            combined.to_zarr(
                zarr_path,
                mode="w",
                encoding=encoding,
                consolidated=True,
            )

        logger.info(f"Zarr store created/updated at {zarr_path}")
        return zarr_path

    def operational_update(self, output_dir: Path) -> None:
        """Run operational update for NAM pressure levels.

        Args:
            output_dir: Output directory for Zarr store
        """
        logger.info("Starting operational update for NAM CONUS pressure levels")

        # Get latest cycle
        cycle_time, cycle_hour = self.get_latest_cycle()

        # Create temporary directory for GRIB files
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            grib_files = []

            # Download all forecast hours
            for fhour in self.forecast_hours:
                try:
                    grib_file = tmp_path / f"nam_f{fhour:03d}.grib2"
                    self.download_grib_file(cycle_time, cycle_hour, fhour, grib_file)
                    grib_files.append(grib_file)
                except Exception as e:
                    logger.warning(f"Failed to download forecast hour {fhour}: {e}")
                    continue

            if not grib_files:
                msg = "No GRIB files downloaded"
                raise RuntimeError(msg)

            # Convert to Zarr (always create fresh - workflow handles history)
            zarr_path = self.grib_to_zarr(
                grib_files,
                output_dir,
                append=False,
            )

            logger.info(f"Operational update complete: {zarr_path}")
