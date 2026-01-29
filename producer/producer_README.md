# Demand Model Microservice

A synthetic data generation system for simulating a food delivery platform's operational events and reference data. This project generates realistic event streams for orders and riders, along with batch reference data (CSV files) for restaurants, riders, zones, and pricing rules.


## Data Model

### Operational Events (Synthetic Streams)

#### Orders Events
Real-time JSON events representing order lifecycle:

| Field | Description |
|-------|-------------|
| `event_id` | Unique event identifier (UUID) |
| `event_timestamp` | ISO 8601 timestamp |
| `event_type` | `order_created`, `order_accepted`, `order_picked_up`, `order_delivered`, `order_cancelled` |
| `order_id` | Unique order identifier |
| `restaurant_id` | Restaurant reference |
| `rider_id` | Assigned rider (null for created events) |
| `customer_id` | Customer reference |
| `region` | Geographic region |
| `zone_id` | Delivery zone |
| `order_value` | Order amount in ₹ |
| `payment_method` | `card`, `upi`, `cash`, `wallet` |


#### Riders Events
Real-time JSON events for rider lifecycle:

| Field | Description |
|-------|-------------|
| `event_id` | Unique event identifier (UUID) |
| `event_timestamp` | ISO 8601 timestamp |
| `event_type` | `shift_started`, `shift_ended`, `location_ping`, `order_assigned` |
| `rider_id` | Rider reference |
| `zone_id` | Current zone |
| `latitude` | GPS latitude |
| `longitude` | GPS longitude |
| `status` | `idle`, `busy`, `offline` |


### Reference Data (Batch CSV)

Daily snapshots for dimensional data:

| File | Key Fields |
|------|------------|
| `restaurants.csv` | `restaurant_id`, `name`, `zone_id`, `region`, `cuisine_type`, `active_flag` |
| `riders.csv` | `rider_id`, `name`, `phone`, `zone_id`, `rating`, `total_deliveries` |
| `zones.csv` | `zone_id`, `city`, `region`, `boundary_polygon` |
| `pricing_rules.csv` | `rule_id`, `zone_id`, `base_fee`, `per_km_rate`, `valid_from`, `valid_to` |

## Demand Model

The demand model combines multiple statistical components:

```
Effective Rate = λ_base × Day_Multiplier × Hour_Multiplier × Shock_Multiplier
```

### Components

1. **Poisson Base Arrivals**
   - Models random order arrivals following Poisson distribution

2. **Bimodal Gaussian Hourly Pattern**
   - Lunch peak: μ=13:00, σ=1.0 hour
   - Dinner peak: μ=20:00, σ=1.2 hours (wider)
   - Dinner typically 1.0-1.6x lunch volume

3. **Day-of-Week Categorical Weights**
   | Day | Multiplier |
   |-----|------------|
   | Monday | 0.85 |
   | Tuesday | 0.95 |
   | Wednesday | 1.00 (baseline) |
   | Thursday | 1.05 |
   | Friday | 1.20 |
   | Saturday | 1.40 (peak) |
   | Sunday | 1.30 |

4. **Gamma-Distributed Shocks**
   - 5% probability of shock event
   - Gamma distribution (α=2, scale=0.5)
   - Produces 2-5x multipliers for festivals/weather events

## Error Injection

Realistic data quality issues are injected at specified rates:

| Error Type | Rate | Description |
|------------|------|-------------|
| Malformed JSON | 0.5% | Truncated JSON strings |
| Missing Fields | 1.0% | Required fields removed (`order_id`, `event_timestamp`) |
| Null Values | 1.0% | Null in non-nullable fields (`rider_id`, `restaurant_id`) |
| Corrupted Rows | 0.5% | Mangled field values |

## Quick Start

### Prerequisites

```bash
pip install numpy scipy pandas matplotlib seaborn
```

### Generate Data

```bash
cd producer

# Generate synthetic data (1 hour simulation)
python synthetic_data_generator.py
```

Output:
```
============================================================
Delivery Platform Synthetic Data Generator
============================================================

[1/3] Generating reference data...
  - zones: data/reference/zones.csv
  - restaurants: data/reference/restaurants.csv
  - riders: data/reference/riders.csv
  - pricing_rules: data/reference/pricing_rules.csv

[2/3] Generating operational events...
[10:00:00] Generated 145 order events, 412 rider events
[10:10:00] Generated 152 order events, 398 rider events
...

[3/3] Computing statistics...

============================================================
Generation Summary
============================================================
Total Order Events: 8,234
Total Rider Events: 24,567
...

✓ Data generation complete!
  Output directory: /path/to/data
```

### Visualize & Analyze

```bash
# Generate all visualizations and statistics
python visualize.py

# Statistics only (no plots)
python visualize.py --stats-only

# Custom data directory
python visualize.py --data-dir /path/to/data

# Don't save visualization files
python visualize.py --no-save
```

## Visualizations

The visualization script generates:

1. **Order Volume Over Time** - Minute-by-minute event volume
2. **Order Event Types** - Distribution of order lifecycle events
3. **Orders by Region** - Geographic distribution
4. **Order Value Distribution** - Histogram and box plots
5. **Rider Event Types** - Distribution of rider events
6. **Rider Locations** - GPS density heatmap
7. **Rider Status Distribution** - Idle/Busy/Offline breakdown
8. **Error Analysis** - Injected error statistics
9. **Reference Data Stats** - Restaurant, rider, and zone analytics

## API Reference

### DemandModel

```python
from demandmodel import DemandModel

model = DemandModel(base_lambda=50)

# Get order count for a specific minute
orders = model.generate_order_rate(datetime.now())

# Get hourly multiplier
multiplier = model.hourly_multiplier(hour=13)  # Lunch peak

# Get day-of-week multiplier
day_mult = model.get_day_multiplier(datetime.now())

# Generate demand shock
shock = model.demand_shock()
```

### ErrorInjector

```python
from error_injection import ErrorInjector

injector = ErrorInjector()

# Inject errors into an event
modified_event = injector.inject_errors(event)
```

### SyntheticDataGenerator

```python
from synthetic_data_generator import SyntheticDataGenerator

generator = SyntheticDataGenerator(output_dir='data')

# Initialize with reference data
ref_files = generator.initialize()

# Generate events for 1 hour
order_events, rider_events = generator.generate_events(
    start_time=datetime.now(),
    duration_minutes=60,
    output_file='simulation_1h'
)

# Get statistics
stats = generator.get_stats(order_events, rider_events)
```

### DataVisualizer

```python
from visualize import DataVisualizer

viz = DataVisualizer(data_dir='data')
viz.load_events()
viz.load_reference_data()

# Generate all visualizations
viz.generate_all_visualizations()

# Print statistics report
viz.print_stats_report()

# Get stats as dictionary
report = viz.generate_stats_report()
```