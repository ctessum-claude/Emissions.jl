# NEI Processing Pipeline Functions
#
# This module provides mid-level pipeline functions that bridge the gap between
# low-level building blocks and the complete spatial processing workflow for EPA NEI data.

"""
    read_ff10(filepath::String, format::Symbol)

Read an FF10 format emissions file and return the appropriate FF10 DataFrame wrapper.

# Arguments
- `filepath::String`: Path to the FF10 CSV file
- `format::Symbol`: One of `:nonpoint`, `:point`, `:nonroad`, or `:onroad`

# Returns
- FF10*DataFrame object with validated data and unit conversions applied

# Example
```julia
nonpoint_data = read_ff10("nonpoint.csv", :nonpoint)
point_data = read_ff10("point.csv", :point)
```
"""
function read_ff10(filepath::String, format::Symbol)
    raw_data = CSV.read(filepath, DataFrame)

    if format == :nonpoint
        return FF10NonPointDataFrame(raw_data)
    elseif format == :point
        return FF10PointDataFrame(raw_data)
    elseif format == :nonroad
        return FF10NonRoadDataFrame(raw_data)
    elseif format == :onroad
        return FF10OnRoadDataFrame(raw_data)
    else
        throw(ArgumentError("Unknown FF10 format: $format. Must be one of :nonpoint, :point, :nonroad, :onroad"))
    end
end

"""
    aggregate_emissions(emissions_dfs::Vector{EmissionsDataFrame})

Aggregate emissions from multiple FF10 DataFrames by grouping on key columns
(POLID, COUNTRY, FIPS, SCC, coordinates) and summing ANN_VALUE.

# Arguments
- `emissions_dfs::Vector{EmissionsDataFrame}`: Vector of FF10*DataFrame objects

# Returns
- `DataFrame`: Aggregated emissions data grouped by location and source category

# Example
```julia
all_emis = [nonpoint_data, point_data, nonroad_data]
aggregated = aggregate_emissions(all_emis)
```
"""
function aggregate_emissions(emissions_dfs::Vector{T}) where T <: EmissionsDataFrame
    if isempty(emissions_dfs)
        throw(ArgumentError("Cannot aggregate empty vector of emissions data"))
    end

    # Extract all DataFrames
    all_dfs = [emis.df for emis in emissions_dfs]

    # Combine all emissions data
    combined_df = vcat(all_dfs..., cols=:union)

    # Determine grouping columns based on what's available
    grouping_cols = [:POLID, :COUNTRY, :FIPS, :SCC]

    # Add coordinate columns if they exist (for point sources)
    if :LONGITUDE in names(combined_df)
        push!(grouping_cols, :LONGITUDE)
    end
    if :LATITUDE in names(combined_df)
        push!(grouping_cols, :LATITUDE)
    end

    # Group and aggregate
    aggregated = combine(
        groupby(combined_df, grouping_cols),
        :ANN_VALUE => sum => :ANN_VALUE
    )

    return aggregated
end

"""
    filter_known_pollutants(emissions_df::DataFrame)

Filter emissions data to include only pollutants that are defined in the Pollutants dictionary.

# Arguments
- `emissions_df::DataFrame`: Emissions data with POLID column

# Returns
- `DataFrame`: Filtered emissions data containing only known pollutants

# Example
```julia
known_pollutants = filter_known_pollutants(aggregated_emissions)
```
"""
function filter_known_pollutants(emissions_df::DataFrame)
    if !(:POLID in names(emissions_df))
        throw(ArgumentError("DataFrame must have POLID column"))
    end

    return filter(row -> haskey(Pollutants, row.POLID), emissions_df)
end

"""
    map_pollutant_names!(emissions_df::DataFrame)

Map pollutant IDs in the POLID column to standardized names using the Pollutants dictionary.
Modifies the DataFrame in place.

# Arguments
- `emissions_df::DataFrame`: Emissions data with POLID column (modified in place)

# Returns
- `DataFrame`: The same DataFrame with mapped pollutant names

# Example
```julia
map_pollutant_names!(known_pollutants)
```
"""
function map_pollutant_names!(emissions_df::DataFrame)
    if !(:POLID in names(emissions_df))
        throw(ArgumentError("DataFrame must have POLID column"))
    end

    emissions_df[!, :POLID] = [Pollutants[p] for p in emissions_df[!, :POLID]]
    return emissions_df
end

"""
    normalize_country(emissions_df::DataFrame)

Normalize country codes in emissions data (converts "0" to "USA").

# Arguments
- `emissions_df::DataFrame`: Emissions data with COUNTRY column

# Returns
- `DataFrame`: Emissions data with normalized country codes

# Example
```julia
normalized = normalize_country(mapped_emissions)
```
"""
function normalize_country(emissions_df::DataFrame)
    if !(:COUNTRY in names(emissions_df))
        throw(ArgumentError("DataFrame must have COUNTRY column"))
    end

    normalized_df = copy(emissions_df)
    normalized_df[!, :COUNTRY] = [c == "0" ? "USA" : c for c in normalized_df[!, :COUNTRY]]
    return normalized_df
end

"""
    read_gridref(filepaths::Vector{String})

Read and combine multiple grid reference files into a single DataFrame.

# Arguments
- `filepaths::Vector{String}`: Vector of paths to grid reference CSV files

# Returns
- `DataFrame`: Combined grid reference data

# Example
```julia
gridref = read_gridref(["gridref1.csv", "gridref2.csv"])
```
"""
function read_gridref(filepaths::Vector{String})
    if isempty(filepaths)
        throw(ArgumentError("Must provide at least one grid reference file"))
    end

    gridref_dfs = DataFrame[]
    for filepath in filepaths
        if !isfile(filepath)
            @warn "Grid reference file not found: $filepath"
            continue
        end

        df = CSV.read(filepath, DataFrame; comment="#")
        push!(gridref_dfs, df)
    end

    if isempty(gridref_dfs)
        throw(ErrorException("No valid grid reference files found"))
    end

    return vcat(gridref_dfs...; cols=:union)
end

"""
    assign_surrogates(emissions_df::DataFrame, gridref_df::DataFrame;
                     enable_fallback::Bool=true)

Assign surrogate codes to emissions data by joining with grid reference data.
Implements fallback logic using FIPS="00000" for national-level surrogates
when county-specific surrogates are not available.

# Arguments
- `emissions_df::DataFrame`: Emissions data with COUNTRY, FIPS, SCC columns
- `gridref_df::DataFrame`: Grid reference data with COUNTRY, FIPS, SCC, Surrogate columns
- `enable_fallback::Bool`: Whether to use national fallback when county-specific surrogate missing

# Returns
- `DataFrame`: Emissions data with Surrogate column added

# Example
```julia
with_surrogates = assign_surrogates(normalized_emissions, gridref)
```
"""
function assign_surrogates(emissions_df::DataFrame, gridref_df::DataFrame;
                          enable_fallback::Bool=true)
    required_emis_cols = [:COUNTRY, :FIPS, :SCC]
    required_gridref_cols = [:COUNTRY, :FIPS, :SCC, :Surrogate]

    missing_emis = setdiff(required_emis_cols, names(emissions_df))
    missing_gridref = setdiff(required_gridref_cols, names(gridref_df))

    if !isempty(missing_emis)
        throw(ArgumentError("Emissions DataFrame missing columns: $missing_emis"))
    end
    if !isempty(missing_gridref)
        throw(ArgumentError("Grid reference DataFrame missing columns: $missing_gridref"))
    end

    # First try: exact match on COUNTRY, FIPS, SCC
    joined = leftjoin(emissions_df, gridref_df, on=[:COUNTRY, :FIPS, :SCC])

    if !enable_fallback
        return joined
    end

    # Second try: fallback to national surrogates (FIPS="00000") for unmatched records
    unmatched_idx = ismissing.(joined.Surrogate)

    if any(unmatched_idx)
        unmatched = joined[unmatched_idx, :]

        # Create fallback gridref with FIPS="00000" for national surrogates
        fallback_gridref = copy(gridref_df)
        fallback_gridref[!, :FIPS] .= "00000"

        # Try matching with fallback
        fallback_joined = leftjoin(
            unmatched[:, Not(:Surrogate)],
            fallback_gridref,
            on=[:COUNTRY, :FIPS, :SCC]
        )

        # Update the main results with fallback matches
        fallback_matched = .!ismissing.(fallback_joined.Surrogate)
        if any(fallback_matched)
            joined[unmatched_idx, :Surrogate][fallback_matched] = fallback_joined.Surrogate[fallback_matched]
        end
    end

    return joined
end

"""
    build_data_weight_map(surrogate_specs::Vector{SurrogateSpec})

Build a mapping of unique (data_shapefile, weight_shapefile) pairs from surrogate specifications.
This is used to optimize sparse matrix generation by processing each unique pair only once.

# Arguments
- `surrogate_specs::Vector{SurrogateSpec}`: Vector of surrogate specifications

# Returns
- `Dict{Tuple{String,String}, Vector{Int}}`: Maps (data_file, weight_file) to surrogate indices

# Example
```julia
data_weight_map = build_data_weight_map(spatial_processor.SrgSpecs)
```
"""
function build_data_weight_map(surrogate_specs::Vector{SurrogateSpec})
    data_weight_map = Dict{Tuple{String,String}, Vector{Int}}()

    for (i, spec) in enumerate(surrogate_specs)
        key = (spec.DataShapefile, spec.WeightShapefile)
        if haskey(data_weight_map, key)
            push!(data_weight_map[key], i)
        else
            data_weight_map[key] = [i]
        end
    end

    return data_weight_map
end

"""
    find_surrogate_by_code(code::Int, surrogate_specs::Vector{SurrogateSpec},
                          region::String="USA")

Find a surrogate specification by code, with optional region filtering.

# Arguments
- `code::Int`: Surrogate code to search for
- `surrogate_specs::Vector{SurrogateSpec}`: Vector of surrogate specifications
- `region::String`: Region to filter by (default "USA")

# Returns
- `Union{SurrogateSpec, Nothing}`: Matching surrogate spec or nothing if not found

# Example
```julia
pop_surrogate = find_surrogate_by_code(100, spatial_processor.SrgSpecs)
```
"""
function find_surrogate_by_code(code::Int, surrogate_specs::Vector{SurrogateSpec},
                               region::String="USA")
    for spec in surrogate_specs
        if spec.Code == code && spec.Region == region
            return spec
        end
    end
    return nothing
end