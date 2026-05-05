# NOAA NAM CONUS Pressure Levels Forecast

![Status](https://img.shields.io/badge/status-updating-success)

Automated conversion of NOAA North American Mesoscale (NAM) CONUS pressure level forecast data to cloud-optimized Zarr format.

---

## Dataset Specifications

- **Spatial domain**: Continental United States (CONUS)
- **Spatial resolution**: 12.19 km
- **Projection**: Lambert Conformal Conic
- **Grid dimensions**: 614 × 428 points
- **Temporal resolution**: Updates 4 times daily (00Z, 06Z, 12Z, 18Z)
- **Forecast range**: 0-48 hours (2 days)
- **Vertical levels**: 21 pressure levels (1000-100 hPa)

---

## About

The **North American Mesoscale (NAM)** model is a regional weather prediction system run by NOAA's National Centers for Environmental Prediction (NCEP). This dataset provides pressure level forecast data in cloud-optimized Zarr format, updated automatically every 6 hours.

### Key Variables

- **TMP**: Temperature (K)
- **RH**: Relative Humidity (%)
- **UGRD**: U-component of Wind (m/s)
- **VGRD**: V-component of Wind (m/s)
- **HGT**: Geopotential Height (gpm)
- **VVEL**: Vertical Velocity (Pa/s)
- **ABSV**: Absolute Vorticity (1/s)

### Pressure Levels

1000, 975, 950, 925, 900, 850, 800, 750, 700, 650, 600, 550, 500, 450, 400, 350, 300, 250, 200, 150, 100 hPa

---

## Usage

### Python

```python
import xarray as xr

# Open the dataset
ds = xr.open_zarr('data/nam_conus_pressure_levels.zarr', consolidated=True)

# Select data at 500 hPa level
data_500mb = ds.sel(level=500)

# Get temperature at specific time
temp = ds['TMP'].isel(init_time=0, time=0)
```

### Command Line

```bash
# List available datasets
nam-zarr list-datasets

# Get dataset info
nam-zarr info --dataset-id noaa-nam-conus-pressure-levels

# Run manual update
nam-zarr operational-update --dataset-id noaa-nam-conus-pressure-levels --verbose
```

---

## Installation

```bash
# Clone the repository
git clone https://github.com/andrewnakas/nam_Pressure_levels.git
cd nam_Pressure_levels

# Install with uv
uv pip install -e .

# Or with pip
pip install -e .
```

---

## Automation

This repository uses GitHub Actions to automatically:

1. Download the latest NAM pressure level forecast data from NOAA NOMADS
2. Convert GRIB2 files to Zarr format
3. Maintain a rolling dataset (keeps only the latest forecast)
4. Generate metadata catalogs
5. Deploy to GitHub Pages

The workflow runs every 6 hours and automatically manages git history to keep the repository size small.

---

## Data Source

- **Provider**: NOAA/NCEP
- **Source**: [NOMADS](https://nomads.ncep.noaa.gov)
- **Documentation**: [NAM Information](https://www.nco.ncep.noaa.gov/pmb/products/nam/)

---

## Related Datasets

- [NOAA NBM CONUS](https://github.com/andrewnakas/Nbm_to_zarr) - National Blend of Models at 2.5 km
- [NOAA HRRR Alaska](https://github.com/andrewnakas/ak_hrrr_to_zarr) - 3 km Alaska regional
- [NOAA GFS Forecast](https://github.com/andrewnakas/ak_hrrr_to_zarr) - Global 0.25° resolution

---

## License

MIT License - See [LICENSE](LICENSE) file for details.

---

## Acknowledgments

Data provided by NOAA/NCEP. This is an independent project and is not affiliated with or endorsed by NOAA.
