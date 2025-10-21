"""Pydantic models for PLEXOS simulation configuration objects."""

from typing import Annotated

from pydantic import Field

from .component import PLEXOSConfiguration


class PLEXOSMTSchedule(PLEXOSConfiguration):
    """PLEXOS MT Schedule (Medium-Term Schedule) configuration."""

    step_type: Annotated[
        int,
        Field(
            alias="Step Type",
            description="Step type: Day (1), Week (2), Month (3), Year (4)",
            json_schema_extra={"enum": [1, 2, 3, 4]},
        ),
    ] = 4
    at_a_time: Annotated[
        int,
        Field(
            alias="At a Time",
            description="Number of steps to process at a time",
            ge=1,
        ),
    ] = 1
    ldc_type: Annotated[
        int,
        Field(
            alias="LDC Type",
            description="Load duration curve type",
            ge=0,
        ),
    ] = 3
    block_count: Annotated[
        int,
        Field(
            alias="Block Count",
            description="Number of blocks for load duration curve",
            ge=1,
        ),
    ] = 6
    ldc_slicing_method: Annotated[
        int,
        Field(
            alias="LDC Slicing Method",
            description="Method for slicing load duration curve",
            ge=0,
        ),
    ] = 0
    heat_rate_detail: Annotated[
        int,
        Field(
            alias="Heat Rate Detail",
            description="Level of heat rate detail (2 = simplest)",
            ge=0,
        ),
    ] = 2
    transmission_detail: Annotated[
        int,
        Field(
            alias="Transmission Detail",
            description="Transmission modeling detail: Regional (0), Zonal (1), Nodal (2)",
            json_schema_extra={"enum": [0, 1, 2]},
        ),
    ] = 0

    @classmethod
    def example(cls) -> "PLEXOSMTSchedule":
        """Create an example MT Schedule configuration."""
        return PLEXOSMTSchedule(
            name="MT_Schedule_Example",
            step_type=4,
            at_a_time=1,
            ldc_type=3,
            block_count=6,
            ldc_slicing_method=0,
            heat_rate_detail=2,
            transmission_detail=0,
        )


class PLEXOSSTSchedule(PLEXOSConfiguration):
    """PLEXOS ST Schedule (Short-Term Schedule) configuration."""

    transmission_detail: Annotated[
        int,
        Field(
            alias="Transmission Detail",
            description="Transmission modeling detail: Regional (0), Zonal (1), Nodal (2)",
            json_schema_extra={"enum": [0, 1, 2]},
        ),
    ] = 1
    stochastic_method: Annotated[
        int,
        Field(
            alias="Stochastic Method",
            description="Stochastic optimization method",
            ge=0,
        ),
    ] = 0
    heat_rate_detail: Annotated[
        int,
        Field(
            alias="Heat Rate Detail",
            description="Level of heat rate detail",
            ge=0,
        ),
    ] = 2

    @classmethod
    def example(cls) -> "PLEXOSSTSchedule":
        """Create an example ST Schedule configuration."""
        return PLEXOSSTSchedule(
            name="ST_Schedule_Example",
            transmission_detail=1,
            stochastic_method=0,
            heat_rate_detail=2,
        )


class PLEXOSProduction(PLEXOSConfiguration):
    """PLEXOS Production simulation configuration."""

    unit_commitment_optimality: Annotated[
        int,
        Field(
            alias="Unit Commitment Optimality",
            description="Unit commitment method: LP (0), Rounded Relaxation (1), MIP (2)",
            json_schema_extra={"enum": [0, 1, 2]},
        ),
    ] = 2
    rounding_up_threshold: Annotated[
        float,
        Field(
            alias="Rounding Up Threshold",
            description="Threshold for rounding up unit commitment decisions",
            ge=0.0,
            le=1.0,
        ),
    ] = 0.26
    max_heat_rate_tranches: Annotated[
        int,
        Field(
            alias="Max Heat Rate Tranches",
            description="Maximum number of heat rate tranches",
            ge=1,
        ),
    ] = 1

    @classmethod
    def example(cls) -> "PLEXOSProduction":
        """Create an example Production configuration."""
        return PLEXOSProduction(
            name="Production_Example",
            unit_commitment_optimality=2,
            rounding_up_threshold=0.26,
            max_heat_rate_tranches=1,
        )


class PLEXOSPASA(PLEXOSConfiguration):
    """PLEXOS PASA (Projected Assessment of System Adequacy) configuration."""

    step_type: Annotated[
        int,
        Field(
            alias="Step Type",
            description="Step type: Day (1), Week (2), Month (3), Year (4)",
            json_schema_extra={"enum": [1, 2, 3, 4]},
        ),
    ] = 1
    write_outage_text_files: Annotated[
        int,
        Field(
            alias="Write Outage Text Files",
            description="Write outage text files: Yes (-1), No (0)",
            json_schema_extra={"enum": [-1, 0]},
        ),
    ] = -1

    @classmethod
    def example(cls) -> "PLEXOSPASA":
        """Create an example PASA configuration."""
        return PLEXOSPASA(
            name="PASA_Example",
            step_type=1,
            write_outage_text_files=-1,
        )


class PLEXOSPerformance(PLEXOSConfiguration):
    """PLEXOS Performance configuration for solver settings."""

    solver: Annotated[
        int,
        Field(
            alias="SOLVER",
            description="Solver selection: CPLEX (1), Xpress (2), CBC (3), Gurobi (4), MOSEK (5), HiGHS (6)",
            json_schema_extra={"enum": [1, 2, 3, 4, 5, 6]},
        ),
    ] = 4
    mip_relative_gap: Annotated[
        float,
        Field(
            alias="MIP Relative Gap",
            description="MIP solver relative optimality gap tolerance",
            ge=0.0,
        ),
    ] = 0.01
    mip_maximum_threads: Annotated[
        int,
        Field(
            alias="MIP Maximum Threads",
            description="Maximum number of threads for MIP solver",
            ge=1,
        ),
    ] = 20
    mip_max_time: Annotated[
        int,
        Field(
            alias="MIP Max Time",
            description="Maximum time in seconds for MIP solver",
            ge=0,
        ),
    ] = 6500

    @classmethod
    def example(cls) -> "PLEXOSPerformance":
        """Create an example Performance configuration."""
        return PLEXOSPerformance(
            name="Performance_Example",
            solver=4,
            mip_relative_gap=0.01,
            mip_maximum_threads=20,
            mip_max_time=6500,
        )


class PLEXOSReport(PLEXOSConfiguration):
    """PLEXOS Report configuration for output settings."""

    output_results_by_day: Annotated[
        int,
        Field(
            alias="Output Results by Day",
            description="Output results aggregated by day: Yes (-1), No (0)",
            json_schema_extra={"enum": [-1, 0]},
        ),
    ] = -1
    output_results_by_month: Annotated[
        int,
        Field(
            alias="Output Results by Month",
            description="Output results aggregated by month: Yes (-1), No (0)",
            json_schema_extra={"enum": [-1, 0]},
        ),
    ] = -1
    output_results_by_fiscal_year: Annotated[
        int,
        Field(
            alias="Output Results by Fiscal Year",
            description="Output results aggregated by fiscal year: Yes (-1), No (0)",
            json_schema_extra={"enum": [-1, 0]},
        ),
    ] = -1

    @classmethod
    def example(cls) -> "PLEXOSReport":
        """Create an example Report configuration."""
        return PLEXOSReport(
            name="Report_Example",
            output_results_by_day=-1,
            output_results_by_month=-1,
            output_results_by_fiscal_year=-1,
        )


class PLEXOSTransmission(PLEXOSConfiguration):
    """PLEXOS Transmission configuration for network modeling."""

    of_method: Annotated[
        int,
        Field(
            alias="OF Method",
            description="Optimal power flow method: DC OF (0), Fixed Shift Factor (1), Iterative (2), Full AC (3)",
            json_schema_extra={"enum": [0, 1, 2, 3]},
        ),
    ] = 1
    constraint_voltage_threshold: Annotated[
        float,
        Field(
            alias="Constraint Voltage Threshold",
            description="Voltage threshold for constraint monitoring",
            ge=0.0,
        ),
    ] = 0.0
    ptdf_method: Annotated[
        int,
        Field(
            alias="PTDF Method",
            description="Power Transfer Distribution Factor calculation method",
            json_schema_extra={"enum": [0, 1, 2]},
        ),
    ] = 1
    bound_node_phase_angles: Annotated[
        int,
        Field(
            alias="Bound Node Phase Angles",
            description="Bound node phase angles: Yes (-1), No (0)",
            json_schema_extra={"enum": [-1, 0]},
        ),
    ] = 0

    @classmethod
    def example(cls) -> "PLEXOSTransmission":
        """Create an example Transmission configuration."""
        return PLEXOSTransmission(
            name="Transmission_Example",
            of_method=1,
            constraint_voltage_threshold=0.0,
            ptdf_method=1,
            bound_node_phase_angles=0,
        )


class PLEXOSDiagnostic(PLEXOSConfiguration):
    """PLEXOS Diagnostic configuration for debugging and analysis.

    This class serves as a container for diagnostic settings.
    Specific attributes can be added as needed.
    """

    @classmethod
    def example(cls) -> "PLEXOSDiagnostic":
        """Create an example Diagnostic configuration."""
        return PLEXOSDiagnostic(name="Diagnostic_Example")
