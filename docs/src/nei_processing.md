# NEI Processing

## Overview

The Emissions.jl package provides tools for processing EPA National Emissions Inventory (NEI)
data. It supports reading FF10-format emissions files, spatial allocation using surrogate
shapefiles, and gridding emissions to model-ready formats.

The package also includes plume rise calculations based on ASME (1973), as described in
Seinfeld and Pandis, "Atmospheric Chemistry and Physics - From Air Pollution to Climate Change".

## Plume Rise

The following functions implement the ASME (1973) plume rise algorithm:

- `findLayer` - Find the model layer containing a given height
- `calcDeltaH` - Calculate plume rise
- `ASME` - Calculate effective emissions height with plume rise
- `calcDeltaHPrecomputed` - Calculate plume rise with precomputed meteorological parameters
- `ASMEPrecomputed` - Calculate effective emissions height with precomputed parameters

## Constants and Unit Conversions

```@docs
tonperyear
tonpermonth
foot
kelvin
Pollutants
```

## Data Types

```@docs
EmissionsDataFrame
SurrogateSpec
GridDef
SpatialProcessor
Config
IndexInfo
```

## FF10 Data Formats

The EPA FF10 (Flat File 10) format is the standard format for emissions inventory data.
The package supports four FF10 format types:

```@docs
FF10NonPointDataFrame
FF10PointDataFrame
FF10NonRoadDataFrame
FF10OnRoadDataFrame
```

## I/O Functions

```@docs
strip_missing
getCountry
read_grid
getShapefilePath
validateShapefile
readSrgSpecSMOKE
NewSpatialProcessor
```

## Spatial Processing

```@docs
NewPolygon
NewGridIrregular
setupSpatialProcessor
findCountyPolygon
GetIndex
recordToGrid
GridFactors
uniqueCoordinates
uniqueLoc
```

## Surrogate Operations

```@docs
generate_data_sparse_matrices
generate_weight_sparse_matrices
generate_grid_sparse_matrices
generate_countySurrogate
update_locIndex
```

## Pipeline Functions

The following functions provide mid-level pipeline operations that connect the low-level building blocks into a complete NEI processing workflow:

```@docs
read_ff10
aggregate_emissions
filter_known_pollutants
map_pollutant_names!
normalize_country
read_gridref
assign_surrogates
build_data_weight_map
find_surrogate_by_code
```

## Output

```@docs
get_data_weight_shapefiles
writeEmis
```

## Example Workflow

Here's an example of how to use the pipeline functions to process NEI data:

```julia
using Emissions

# Step 1: Read FF10 files
nonpoint_data = read_ff10("2019_nonpoint.csv", :nonpoint)
point_data = read_ff10("2019_point.csv", :point)

# Step 2: Aggregate emissions from multiple sources
all_emissions = aggregate_emissions([nonpoint_data, point_data])

# Step 3: Filter to known pollutants and normalize names
known_pollutants = filter_known_pollutants(all_emissions)
map_pollutant_names!(known_pollutants)

# Step 4: Normalize country codes
normalized_emissions = normalize_country(known_pollutants)

# Step 5: Read grid reference and assign surrogates
gridref = read_gridref(["gridref_usa.csv"])
emissions_with_surrogates = assign_surrogates(normalized_emissions, gridref)

# Step 6: Create spatial processor and grid emissions
config = Config(
    ["gridref_usa.csv"],
    "surrogate_spec.csv",
    "/path/to/surrogates/",
    "+proj=longlat +datum=WGS84",
    "+proj=lcc +lat_1=33 +lat_2=45",
    "grid.txt",
    "InMAP",
    "/path/to/counties.shp",
    "/path/to/output/"
)

spatial_processor = setupSpatialProcessor(config)
writeEmis(emissions_with_surrogates, spatial_processor, "output_emissions.shp")
```
