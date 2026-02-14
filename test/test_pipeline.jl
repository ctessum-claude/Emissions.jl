using Test
using Emissions
using DataFrames
using CSV
using Unitful

@testset "Pipeline Functions" begin
    @testset "read_ff10" begin
        # Create temporary test files
        temp_dir = mktempdir()

        # Create synthetic nonpoint data
        nonpoint_data = DataFrame(
            COUNTRY = ["0", "0"],
            FIPS = ["36001", "36005"],
            TRIBAL_CODE = ["0", "0"],
            CENSUS_TRACT = ["0", "0"],
            SHAPE_ID = ["0", "0"],
            SCC = ["2103007000", "2103007000"],
            EMIS_TYPE = ["", ""],
            POLID = ["NOX", "VOC"],
            ANN_VALUE = [150.5, 75.2],
            ANN_PCT_RED = [0.0, 0.0],
            CONTROL_IDS = ["", ""],
            CONTROL_MEASURES = ["", ""],
            CURRENT_COST = [0.0, 0.0],
            CUMULATIVE_COST = [0.0, 0.0],
            PROJECTION_FACTOR = [1.0, 1.0],
            REG_CODES = ["", ""],
            CALC_METHOD = [1, 1],
            CALC_YEAR = [2019, 2019],
            DATE_UPDATED = ["", ""],
            DATA_SET_ID = ["", ""],
            JAN_VALUE = [0.0, 0.0],
            FEB_VALUE = [0.0, 0.0],
            MAR_VALUE = [0.0, 0.0],
            APR_VALUE = [0.0, 0.0],
            MAY_VALUE = [0.0, 0.0],
            JUN_VALUE = [0.0, 0.0],
            JUL_VALUE = [0.0, 0.0],
            AUG_VALUE = [0.0, 0.0],
            SEP_VALUE = [0.0, 0.0],
            OCT_VALUE = [0.0, 0.0],
            NOV_VALUE = [0.0, 0.0],
            DEC_VALUE = [0.0, 0.0],
            JAN_PCTRED = [0.0, 0.0],
            FEB_PCTRED = [0.0, 0.0],
            MAR_PCTRED = [0.0, 0.0],
            APR_PCTRED = [0.0, 0.0],
            MAY_PCTRED = [0.0, 0.0],
            JUN_PCTRED = [0.0, 0.0],
            JUL_PCTRED = [0.0, 0.0],
            AUG_PCTRED = [0.0, 0.0],
            SEP_PCTRED = [0.0, 0.0],
            OCT_PCTRED = [0.0, 0.0],
            NOV_PCTRED = [0.0, 0.0],
            DEC_PCTRED = [0.0, 0.0],
            COMMENT = ["Test", "Test"]
        )

        nonpoint_file = joinpath(temp_dir, "nonpoint.csv")
        CSV.write(nonpoint_file, nonpoint_data)

        # Test reading nonpoint data
        result = read_ff10(nonpoint_file, :nonpoint)
        @test result isa FF10NonPointDataFrame
        @test size(result.df, 1) == 2
        @test haskey(Emissions.Pollutants, result.df[1, :POLID])  # Should find mapped pollutant

        # Test unknown format
        @test_throws ArgumentError read_ff10(nonpoint_file, :unknown)

        # Test nonexistent file
        @test_throws ArgumentError read_ff10("nonexistent.csv", :nonpoint)

        # Cleanup
        rm(temp_dir, recursive=true)
    end

    @testset "aggregate_emissions" begin
        # Create synthetic FF10 data
        data1 = DataFrame(
            POLID = ["NOX", "VOC"],
            COUNTRY = ["USA", "USA"],
            FIPS = ["36001", "36001"],
            SCC = ["2103007000", "2103007000"],
            ANN_VALUE = [100.0u"kg/s", 50.0u"kg/s"]
        )

        data2 = DataFrame(
            POLID = ["NOX", "SO2"],
            COUNTRY = ["USA", "USA"],
            FIPS = ["36001", "36005"],
            SCC = ["2103007000", "2103007000"],
            ANN_VALUE = [200.0u"kg/s", 75.0u"kg/s"]
        )

        # Mock FF10 DataFrames
        struct MockFF10DataFrame <: EmissionsDataFrame
            df::DataFrame
        end

        emis1 = MockFF10DataFrame(data1)
        emis2 = MockFF10DataFrame(data2)

        # Test aggregation
        result = aggregate_emissions([emis1, emis2])
        @test result isa DataFrame
        @test nrow(result) == 3  # NOX from 36001 should be aggregated, others separate

        # Find the NOX record for FIPS 36001 - should be sum of 100 + 200 = 300
        nox_36001 = filter(r -> r.POLID == "NOX" && r.FIPS == "36001", result)[1, :]
        @test nox_36001.ANN_VALUE == 300.0u"kg/s"

        # Test empty input
        @test_throws ArgumentError aggregate_emissions(EmissionsDataFrame[])
    end

    @testset "filter_known_pollutants" begin
        test_data = DataFrame(
            POLID = ["NOX", "UNKNOWN_POLLUTANT", "VOC", "ANOTHER_UNKNOWN"],
            ANN_VALUE = [100.0, 200.0, 150.0, 75.0]
        )

        result = filter_known_pollutants(test_data)
        @test nrow(result) == 2  # Only NOX and VOC should remain
        @test all(haskey(Emissions.Pollutants, p) for p in result.POLID)

        # Test missing POLID column
        bad_data = DataFrame(WRONG_COL = ["A", "B"])
        @test_throws ArgumentError filter_known_pollutants(bad_data)
    end

    @testset "map_pollutant_names!" begin
        test_data = DataFrame(
            POLID = ["NOX", "VOC", "SO2"],
            ANN_VALUE = [100.0, 200.0, 150.0]
        )

        original_data = copy(test_data)
        result = map_pollutant_names!(test_data)

        # Should modify in place
        @test result === test_data
        @test test_data.POLID == [Emissions.Pollutants[p] for p in original_data.POLID]

        # Test missing POLID column
        bad_data = DataFrame(WRONG_COL = ["A", "B"])
        @test_throws ArgumentError map_pollutant_names!(bad_data)
    end

    @testset "normalize_country" begin
        test_data = DataFrame(
            COUNTRY = ["0", "USA", "CAN", "0"],
            FIPS = ["36001", "36005", "48001", "12001"]
        )

        result = normalize_country(test_data)
        @test result.COUNTRY == ["USA", "USA", "CAN", "USA"]
        @test result !== test_data  # Should return new DataFrame

        # Test missing COUNTRY column
        bad_data = DataFrame(WRONG_COL = ["A", "B"])
        @test_throws ArgumentError normalize_country(bad_data)
    end

    @testset "read_gridref" begin
        temp_dir = mktempdir()

        # Create test grid reference files
        gridref1 = DataFrame(
            COUNTRY = ["USA", "USA"],
            FIPS = ["36001", "36005"],
            SCC = ["2103007000", "2103007000"],
            Surrogate = [100, 200]
        )

        gridref2 = DataFrame(
            COUNTRY = ["USA"],
            FIPS = ["48001"],
            SCC = ["2103007000"],
            Surrogate = [300]
        )

        file1 = joinpath(temp_dir, "gridref1.csv")
        file2 = joinpath(temp_dir, "gridref2.csv")

        CSV.write(file1, gridref1)
        CSV.write(file2, gridref2)

        # Test reading multiple files
        result = read_gridref([file1, file2])
        @test nrow(result) == 3  # Should combine both files
        @test sort(result.Surrogate) == [100, 200, 300]

        # Test empty file list
        @test_throws ArgumentError read_gridref(String[])

        # Test nonexistent files
        @test_throws ErrorException read_gridref(["nonexistent.csv"])

        # Cleanup
        rm(temp_dir, recursive=true)
    end

    @testset "assign_surrogates" begin
        emissions_data = DataFrame(
            COUNTRY = ["USA", "USA", "USA", "USA"],
            FIPS = ["36001", "36001", "36005", "99999"],  # 99999 doesn't exist in gridref
            SCC = ["2103007000", "2205007000", "2103007000", "2103007000"],
            POLID = ["NOX", "VOC", "SO2", "NH3"],
            ANN_VALUE = [100.0, 200.0, 150.0, 75.0]
        )

        gridref_data = DataFrame(
            COUNTRY = ["USA", "USA", "USA", "USA"],
            FIPS = ["36001", "36005", "36001", "00000"],  # Include national fallback
            SCC = ["2103007000", "2103007000", "2205007000", "2103007000"],
            Surrogate = [100, 200, 300, 999]  # 999 is national fallback for unknown FIPS
        )

        # Test with fallback enabled (default)
        result = assign_surrogates(emissions_data, gridref_data)
        @test nrow(result) == 4
        @test !any(ismissing.(result.Surrogate))  # All should have surrogates

        # The 99999 FIPS should get the national fallback surrogate (999)
        fallback_record = filter(r -> r.FIPS == "99999", result)[1, :]
        @test fallback_record.Surrogate == 999

        # Test without fallback
        result_no_fallback = assign_surrogates(emissions_data, gridref_data; enable_fallback=false)
        @test any(ismissing.(result_no_fallback.Surrogate))  # 99999 FIPS should have missing surrogate

        # Test missing columns
        bad_emissions = DataFrame(WRONG_COL = ["A"])
        @test_throws ArgumentError assign_surrogates(bad_emissions, gridref_data)

        bad_gridref = DataFrame(WRONG_COL = ["A"])
        @test_throws ArgumentError assign_surrogates(emissions_data, bad_gridref)
    end

    @testset "build_data_weight_map" begin
        # Create test surrogate specs
        spec1 = SurrogateSpec("USA", "Pop", 100, "pop.shp", "POP", "area.shp",
                             "Population", [""], ["AREA"], [1.0], "", [""], Float64[])
        spec2 = SurrogateSpec("USA", "Roads", 200, "roads.shp", "LENGTH", "area.shp",
                             "Roads", [""], ["AREA"], [1.0], "", [""], Float64[])
        spec3 = SurrogateSpec("USA", "Pop2", 300, "pop.shp", "POP", "area.shp",  # Same files as spec1
                             "Population variant", [""], ["AREA"], [1.0], "", [""], Float64[])

        specs = [spec1, spec2, spec3]
        result = build_data_weight_map(specs)

        @test result isa Dict{Tuple{String,String}, Vector{Int}}
        @test length(result) == 2  # Two unique (data, weight) combinations

        # spec1 and spec3 should map to same key
        pop_area_key = ("pop.shp", "area.shp")
        @test haskey(result, pop_area_key)
        @test sort(result[pop_area_key]) == [1, 3]

        # spec2 should have its own key
        roads_area_key = ("roads.shp", "area.shp")
        @test haskey(result, roads_area_key)
        @test result[roads_area_key] == [2]
    end

    @testset "find_surrogate_by_code" begin
        # Create test surrogate specs
        spec_usa = SurrogateSpec("USA", "Pop", 100, "pop.shp", "POP", "area.shp",
                                "Population", [""], ["AREA"], [1.0], "", [""], Float64[])
        spec_can = SurrogateSpec("CAN", "Pop", 100, "pop_can.shp", "POP", "area_can.shp",
                                "Population Canada", [""], ["AREA"], [1.0], "", [""], Float64[])
        spec_usa2 = SurrogateSpec("USA", "Roads", 200, "roads.shp", "LENGTH", "area.shp",
                                 "Roads", [""], ["AREA"], [1.0], "", [""], Float64[])

        specs = [spec_usa, spec_can, spec_usa2]

        # Test finding by code with default region
        result = find_surrogate_by_code(100, specs)
        @test result === spec_usa  # Should find USA version by default

        # Test finding by code with specific region
        result_can = find_surrogate_by_code(100, specs, "CAN")
        @test result_can === spec_can

        # Test finding different code
        result_roads = find_surrogate_by_code(200, specs)
        @test result_roads === spec_usa2

        # Test not found
        result_missing = find_surrogate_by_code(999, specs)
        @test result_missing === nothing

        # Test not found for region
        result_missing_region = find_surrogate_by_code(200, specs, "CAN")
        @test result_missing_region === nothing
    end
end