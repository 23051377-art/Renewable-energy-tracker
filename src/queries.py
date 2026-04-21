def q_state_year_summary():
    query = """
    SELECT
        year,
        state,
        COUNT(*) AS record_count,
        ROUND(SUM(total_energy), 2) AS total_energy,
        ROUND(AVG(efficiency), 4) AS avg_efficiency,
        ROUND(AVG(capacity_utilization_pct), 2) AS avg_capacity_utilization
    FROM energy_data
    GROUP BY year, state
    ORDER BY year, total_energy DESC
    """
    return "State wise yearly energy summary", query, ()

def q_top_states_by_year(year, limit=5):
    query = """
    SELECT
        state,
        ROUND(SUM(total_energy), 2) AS total_energy,
        ROUND(AVG(efficiency), 4) AS avg_efficiency,
        ROUND(AVG(profit_margin_pct), 2) AS avg_profit_margin
    FROM energy_data
    WHERE year = ?
    GROUP BY state
    ORDER BY total_energy DESC
    LIMIT ?
    """
    return f"Top {limit} states in {year}", query, (year, limit)

def q_plant_type_summary():
    query = """
    SELECT
        plant_type,
        COUNT(*) AS record_count,
        ROUND(SUM(total_energy), 2) AS total_energy,
        ROUND(AVG(capacity_utilization_pct), 2) AS avg_capacity_utilization,
        ROUND(AVG(profit_margin_pct), 2) AS avg_profit_margin
    FROM energy_data
    GROUP BY plant_type
    ORDER BY total_energy DESC
    """
    return "Plant type summary", query, ()

def q_monthly_trend():
    query = """
    SELECT
        year,
        month,
        ROUND(SUM(total_energy), 2) AS total_energy,
        ROUND(AVG(efficiency), 4) AS avg_efficiency,
        ROUND(AVG(capacity_utilization_pct), 2) AS avg_capacity_utilization
    FROM energy_data
    GROUP BY year, month
    ORDER BY year, month
    """
    return "Monthly trend of energy production", query, ()

def q_year_over_year_growth():
    query = """
    WITH yearly_state AS (
        SELECT
            year,
            state,
            SUM(total_energy) AS total_energy
        FROM energy_data
        GROUP BY year, state
    ),
    growth_data AS (
        SELECT
            year,
            state,
            total_energy,
            LAG(total_energy) OVER (PARTITION BY state ORDER BY year) AS prev_year_energy
        FROM yearly_state
    )
    SELECT
        year,
        state,
        ROUND(total_energy, 2) AS total_energy,
        ROUND(prev_year_energy, 2) AS prev_year_energy,
        ROUND(total_energy - prev_year_energy, 2) AS growth,
        ROUND(
            CASE
                WHEN prev_year_energy IS NULL OR prev_year_energy = 0 THEN NULL
                ELSE ((total_energy - prev_year_energy) * 100.0) / prev_year_energy
            END,
            2
        ) AS growth_pct
    FROM growth_data
    ORDER BY state, year
    """
    return "Year over year growth by state", query, ()

def q_month_over_month_growth():
    query = """
    WITH monthly_state AS (
        SELECT
            year,
            month,
            state,
            SUM(total_energy) AS total_energy
        FROM energy_data
        GROUP BY year, month, state
    ),
    growth_data AS (
        SELECT
            year,
            month,
            state,
            total_energy,
            LAG(total_energy) OVER (PARTITION BY state ORDER BY year, month) AS prev_month_energy
        FROM monthly_state
    )
    SELECT
        year,
        month,
        state,
        ROUND(total_energy, 2) AS total_energy,
        ROUND(prev_month_energy, 2) AS prev_month_energy,
        ROUND(total_energy - prev_month_energy, 2) AS growth,
        ROUND(
            CASE
                WHEN prev_month_energy IS NULL OR prev_month_energy = 0 THEN NULL
                ELSE ((total_energy - prev_month_energy) * 100.0) / prev_month_energy
            END,
            2
        ) AS growth_pct
    FROM growth_data
    ORDER BY state, year, month
    """
    return "Month over month growth by state", query, ()

def q_efficiency_band_analysis():
    query = """
    SELECT
        CASE
            WHEN efficiency < 0.80 THEN 'low'
            WHEN efficiency < 0.90 THEN 'medium'
            ELSE 'high'
        END AS efficiency_band,
        COUNT(*) AS record_count,
        ROUND(AVG(total_energy), 2) AS avg_total_energy,
        ROUND(AVG(capacity_utilization_pct), 2) AS avg_capacity_utilization,
        ROUND(AVG(profit_margin_pct), 2) AS avg_profit_margin
    FROM energy_data
    GROUP BY efficiency_band
    ORDER BY avg_total_energy DESC
    """
    return "Efficiency band analysis", query, ()

def q_sunlight_band_analysis():
    query = """
    SELECT
        CASE
            WHEN sunlight_hours < 6 THEN 'low'
            WHEN sunlight_hours < 9 THEN 'medium'
            ELSE 'high'
        END AS sunlight_band,
        COUNT(*) AS record_count,
        ROUND(AVG(total_energy), 2) AS avg_total_energy,
        ROUND(AVG(efficiency), 4) AS avg_efficiency
    FROM energy_data
    GROUP BY sunlight_band
    ORDER BY avg_total_energy DESC
    """
    return "Sunlight band analysis", query, ()

def q_anomaly_check():
    query = """
    SELECT
        plant_id,
        plant_name,
        state,
        year,
        month,
        ROUND(total_energy, 2) AS total_energy,
        ROUND(source_energy_total, 2) AS source_energy_total,
        ROUND(energy_gap, 2) AS energy_gap
    FROM energy_data
    WHERE ABS(energy_gap) > (0.15 * total_energy)
    ORDER BY ABS(energy_gap) DESC
    """
    return "Potential data anomalies", query, ()

def q_top_plants_by_profit(limit=10):
    query = """
    SELECT
        plant_id,
        plant_name,
        state,
        plant_type,
        ROUND(SUM(revenue), 2) AS total_revenue,
        ROUND(SUM(cost), 2) AS total_cost,
        ROUND(SUM(profit), 2) AS total_profit,
        ROUND(AVG(profit_margin_pct), 2) AS avg_profit_margin
    FROM energy_data
    GROUP BY plant_id, plant_name, state, plant_type
    ORDER BY total_profit DESC
    LIMIT ?
    """
    return f"Top {limit} plants by profit", query, (limit,)

def q_state_quality_score():
    query = """
    SELECT
        state,
        COUNT(*) AS record_count,
        ROUND(AVG(co2_reduction_per_unit), 4) AS avg_co2_reduction_per_unit,
        ROUND(AVG(cost_per_unit_energy), 4) AS avg_cost_per_unit_energy,
        ROUND(AVG(performance_score), 4) AS avg_performance_score
    FROM energy_data
    GROUP BY state
    ORDER BY avg_performance_score DESC
    """
    return "State level performance score", query, ()

def get_reports(latest_year):
    return [
        q_state_year_summary(),
        q_top_states_by_year(latest_year, 5),
        q_plant_type_summary(),
        q_monthly_trend(),
        q_year_over_year_growth(),
        q_month_over_month_growth(),
        q_efficiency_band_analysis(),
        q_sunlight_band_analysis(),
        q_anomaly_check(),
        q_top_plants_by_profit(10),
        q_state_quality_score(),
    ]
