"""
Visualization and Statistics for Synthetic Data

Provides comprehensive visualizations and statistical analysis for
the generated delivery platform data.
"""

import json
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import seaborn as sns
from pathlib import Path
from typing import Dict, List, Any
import argparse
import sys

# Set style
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("husl")


class DataVisualizer:
    """Visualization and statistics for synthetic event data"""
    
    def __init__(self, data_dir: str = 'data') -> None:
        self.data_dir = Path(data_dir)
        self.events_dir = self.data_dir / 'events'
        self.reference_dir = self.data_dir / 'reference'
        self.output_dir = self.data_dir / 'visualizations'
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Data containers
        self.order_events: List[Dict] = []
        self.rider_events: List[Dict] = []
        self.reference_data: Dict[str, pd.DataFrame] = {}
    
    def load_events(self, orders_file: str = 'simulation_1h_orders.json', 
                    riders_file: str = 'simulation_1h_riders.json') -> None:
        """Load event data from JSON files"""
        orders_path = self.events_dir / orders_file
        riders_path = self.events_dir / riders_file
        
        if orders_path.exists():
            with open(orders_path) as f:
                self.order_events = json.load(f)
            print(f"Loaded {len(self.order_events)} order events")
        else:
            print(f"Warning: {orders_path} not found")
        
        if riders_path.exists():
            with open(riders_path) as f:
                self.rider_events = json.load(f)
            print(f"Loaded {len(self.rider_events)} rider events")
        else:
            print(f"Warning: {riders_path} not found")
    
    def load_reference_data(self) -> None:
        """Load reference CSV files"""
        csv_files = ['zones.csv', 'restaurants.csv', 'riders.csv', 'pricing_rules.csv']
        
        for csv_file in csv_files:
            filepath = self.reference_dir / csv_file
            if filepath.exists():
                name = csv_file.replace('.csv', '')
                self.reference_data[name] = pd.read_csv(filepath)
                print(f"Loaded {len(self.reference_data[name])} rows from {csv_file}")
    
    def _prepare_order_df(self) -> pd.DataFrame:
        """Convert order events to DataFrame (excludes malformed string entries)"""
        valid_events = [e for e in self.order_events if isinstance(e, dict)]
        df = pd.DataFrame(valid_events)
        
        if 'event_timestamp' in df.columns:
            df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], errors='coerce')
            df['hour'] = df['event_timestamp'].dt.hour #type: ignore
            df['minute'] = df['event_timestamp'].dt.minute #type: ignore
        
        return df
    
    def _prepare_rider_df(self) -> pd.DataFrame:
        """Convert rider events to DataFrame (excludes malformed string entries)"""
        valid_events = [e for e in self.rider_events if isinstance(e, dict)]
        df = pd.DataFrame(valid_events)
        
        if 'event_timestamp' in df.columns:
            df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], errors='coerce')
            df['hour'] = df['event_timestamp'].dt.hour #type: ignore
            df['minute'] = df['event_timestamp'].dt.minute #type: ignore
        
        return df
    
    # =========================================================================
    # Order Visualizations
    # =========================================================================
    
    def plot_order_volume_over_time(self, save: bool = True) -> None:
        """Plot order volume over time (minute by minute)"""
        df = self._prepare_order_df()
        
        if df.empty or 'event_timestamp' not in df.columns:
            print("No order data available for plotting")
            return
        
        fig, ax = plt.subplots(figsize=(14, 6))
        
        # Group by minute
        df['time_bucket'] = df['event_timestamp'].dt.floor('1min') #type: ignore
        volume = df.groupby('time_bucket').size()
        
        ax.plot(volume.index, volume.values, linewidth=2, color='#2E86AB') #type: ignore
        ax.fill_between(volume.index, volume.values, alpha=0.3, color='#2E86AB') #type: ignore
        
        ax.set_xlabel('Time', fontsize=12)
        ax.set_ylabel('Events per Minute', fontsize=12)
        ax.set_title('Order Event Volume Over Time', fontsize=14, fontweight='bold')
        
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        if save:
            plt.savefig(self.output_dir / 'order_volume_time.png', dpi=150)
            print(f"Saved: {self.output_dir / 'order_volume_time.png'}")
        plt.show()
    
    def plot_order_event_types(self, save: bool = True) -> None:
        """Plot distribution of order event types"""
        df = self._prepare_order_df()
        
        if df.empty or 'event_type' not in df.columns:
            print("No order event type data available")
            return
        
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        
        # Bar chart - filter out null/nan event types and malformed entries
        valid_event_types = ['order_created', 'order_accepted', 'order_picked_up', 'order_delivered', 'order_cancelled']
        df_filtered = df[df['event_type'].isin(valid_event_types)]
        event_counts = df_filtered['event_type'].value_counts()
        colors = sns.color_palette("husl", len(event_counts))
        
        axes[0].bar(event_counts.index, event_counts.values, color=colors)
        axes[0].set_xlabel('Event Type', fontsize=12)
        axes[0].set_ylabel('Count', fontsize=12)
        axes[0].set_title('Order Events by Type', fontsize=14, fontweight='bold')
        axes[0].tick_params(axis='x', rotation=45)
        
        # Pie chart
        axes[1].pie(event_counts.values, labels=event_counts.index, autopct='%1.1f%%',
                   colors=colors, explode=[0.05] * len(event_counts))
        axes[1].set_title('Order Event Distribution', fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        
        if save:
            plt.savefig(self.output_dir / 'order_event_types.png', dpi=150)
            print(f"Saved: {self.output_dir / 'order_event_types.png'}")
        plt.show()
    
    def plot_order_by_region(self, save: bool = True) -> None:
        """Plot orders by region"""
        df = self._prepare_order_df()
        
        if df.empty or 'region' not in df.columns:
            print("No region data available")
            return
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        region_counts = df['region'].value_counts()
        colors = sns.color_palette("Set2", len(region_counts))
        
        bars = ax.barh(region_counts.index, region_counts.values, color=colors) #type: ignore
        ax.set_xlabel('Number of Events', fontsize=12)
        ax.set_ylabel('Region', fontsize=12)
        ax.set_title('Order Events by Region', fontsize=14, fontweight='bold')
        
        # Add value labels
        for bar, val in zip(bars, region_counts.values):
            ax.text(val + 10, bar.get_y() + bar.get_height()/2, 
                   str(val), va='center', fontsize=10)
        
        plt.tight_layout()
        
        if save:
            plt.savefig(self.output_dir / 'order_by_region.png', dpi=150)
            print(f"Saved: {self.output_dir / 'order_by_region.png'}")
        plt.show()
    
    def plot_order_value_distribution(self, save: bool = True) -> None:
        """Plot order value distribution"""
        df = self._prepare_order_df()
        
        if df.empty or 'order_value' not in df.columns:
            print("No order value data available")
            return
        
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        
        # Histogram
        df['order_value'].dropna().hist(bins=50, ax=axes[0], color='#E76F51', edgecolor='white')
        axes[0].set_xlabel('Order Value (INR)', fontsize=12)
        axes[0].set_ylabel('Frequency', fontsize=12)
        axes[0].set_title('Order Value Distribution', fontsize=14, fontweight='bold')
        
        # Box plot by payment method
        if 'payment_method' in df.columns:
            df_clean = df.dropna(subset=['order_value', 'payment_method'])

            # Filter outliers using IQR method
            Q1 = df_clean['order_value'].quantile(0.25)
            Q3 = df_clean['order_value'].quantile(0.75)
            IQR = Q3 - Q1
            df_filtered = df_clean[(df_clean['order_value'] >= Q1 - 1.5 * IQR) & 
                                   (df_clean['order_value'] <= Q3 + 1.5 * IQR)]
            
            df_filtered.boxplot(column='order_value', by='payment_method', ax=axes[1], showfliers=False)
            axes[1].set_xlabel('Payment Method', fontsize=12)
            axes[1].set_ylabel('Order Value (INR)', fontsize=12)
            axes[1].set_title('Order Value by Payment Method', fontsize=14, fontweight='bold')
            plt.suptitle('')  # Remove automatic title
        
        plt.tight_layout()
        
        if save:
            plt.savefig(self.output_dir / 'order_value_dist.png', dpi=150)
            print(f"Saved: {self.output_dir / 'order_value_dist.png'}")
        plt.show()
    
    # =========================================================================
    # Rider Visualizations
    # =========================================================================
    
    def plot_rider_event_types(self, save: bool = True) -> None:
        """Plot rider event type distribution"""
        df = self._prepare_rider_df()
        
        if df.empty or 'event_type' not in df.columns:
            print("No rider event type data available")
            return
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        event_counts = df['event_type'].value_counts()
        colors = sns.color_palette("coolwarm", len(event_counts))
        
        ax.bar(event_counts.index, event_counts.values, color=colors) #type: ignore
        ax.set_xlabel('Event Type', fontsize=12)
        ax.set_ylabel('Count', fontsize=12)
        ax.set_title('Rider Events by Type', fontsize=14, fontweight='bold')
        ax.tick_params(axis='x', rotation=45)
        
        # Add value labels
        for i, (idx, val) in enumerate(zip(event_counts.index, event_counts.values)):
            ax.text(i, val + 50, str(val), ha='center', fontsize=10)
        
        plt.tight_layout()
        
        if save:
            plt.savefig(self.output_dir / 'rider_event_types.png', dpi=150)
            print(f"Saved: {self.output_dir / 'rider_event_types.png'}")
        plt.show()
    
    def plot_rider_locations(self, save: bool = True) -> None:
        """Plot rider location heatmap"""
        df = self._prepare_rider_df()
        
        if df.empty or 'latitude' not in df.columns:
            print("No rider location data available")
            return
        
        fig, ax = plt.subplots(figsize=(12, 10))
        
        # Filter location pings
        location_df = df[df['event_type'] == 'location_ping'].dropna(subset=['latitude', 'longitude'])
        
        if location_df.empty:
            print("No location ping data available")
            return
        
        # Create hexbin plot
        hb = ax.hexbin(location_df['longitude'], location_df['latitude'], 
                       gridsize=30, cmap='YlOrRd', mincnt=1)
        
        ax.set_xlabel('Longitude', fontsize=12)
        ax.set_ylabel('Latitude', fontsize=12)
        ax.set_title('Rider Location Density', fontsize=14, fontweight='bold')
        
        plt.colorbar(hb, ax=ax, label='Event Count')
        plt.tight_layout()
        
        if save:
            plt.savefig(self.output_dir / 'rider_locations.png', dpi=150)
            print(f"Saved: {self.output_dir / 'rider_locations.png'}")
        plt.show()
    
    def plot_rider_status_distribution(self, save: bool = True) -> None:
        """Plot rider status distribution"""
        df = self._prepare_rider_df()
        
        if df.empty or 'status' not in df.columns:
            print("No rider status data available")
            return
        
        fig, ax = plt.subplots(figsize=(8, 8))
        
        status_counts = df['status'].value_counts()
        colors = ['#2ECC71', '#E74C3C', '#95A5A6']
        
        wedges, texts, autotexts = ax.pie( #type: ignore
            status_counts.values, #type: ignore
            labels=status_counts.index, #type: ignore
            autopct='%1.1f%%',
            colors=colors[:len(status_counts)],
            explode=[0.02] * len(status_counts),
            shadow=True
        )
        
        ax.set_title('Rider Status Distribution', fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        
        if save:
            plt.savefig(self.output_dir / 'rider_status_dist.png', dpi=150)
            print(f"Saved: {self.output_dir / 'rider_status_dist.png'}")
        plt.show()
    
    # =========================================================================
    # Error Analysis
    # =========================================================================
    
    def plot_error_analysis(self, save: bool = True) -> None:
        """Analyze and visualize injected errors"""
        order_df = self._prepare_order_df()
        
        fig, ax  = plt.subplots(1, 1, figsize=(10, 5))
        
        order_errors = {
            'Malformed JSON': sum(1 for e in self.order_events if isinstance(e, str)),
            'Null Values': sum(1 for e in self.order_events 
                              if isinstance(e, dict) and any(v is None for k, v in e.items() if k != 'rider_id')),
            'Missing Fields': len(order_df) - order_df.dropna(subset=['order_id', 'event_timestamp'], 
                                                               how='any').shape[0]
        }
        
        colors = ['#E74C3C', '#9B59B6', '#3498DB']
        
        ax.bar(order_errors.keys(), order_errors.values(), color=colors) #type: ignore
        ax.set_xlabel('Error Type', fontsize=12)
        ax.set_ylabel('Count', fontsize=12)
        ax.set_title('Order Event Errors', fontsize=14, fontweight='bold')
        ax.tick_params(axis='x', rotation=30)
        
        # Add percentages
        total_orders = len(self.order_events) if self.order_events else 1
        for i, (error_type, count) in enumerate(order_errors.items()):
            pct = (count / total_orders) * 100
            ax.text(i, count + 5, f'{pct:.2f}%', ha='center', fontsize=9)
        
        
        plt.tight_layout()
        
        if save:
            plt.savefig(self.output_dir / 'error_analysis.png', dpi=150)
            print(f"Saved: {self.output_dir / 'error_analysis.png'}")
        plt.show()
    
    # =========================================================================
    # Reference Data Analysis
    # =========================================================================
    
    def plot_reference_data_stats(self, save: bool = True) -> None:
        """Plot reference data statistics"""
        if not self.reference_data:
            print("No reference data loaded")
            return
        
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        
        # Restaurants by region
        if 'restaurants' in self.reference_data:
            rest_df = self.reference_data['restaurants']
            if 'region' in rest_df.columns:
                region_counts = rest_df['region'].value_counts()
                axes[0, 0].bar(region_counts.index, region_counts.values, color=sns.color_palette("Set2"))
                axes[0, 0].set_xlabel('Region')
                axes[0, 0].set_ylabel('Count')
                axes[0, 0].set_title('Restaurants by Region', fontweight='bold')
            
            if 'cuisine_type' in rest_df.columns:
                cuisine_counts = rest_df['cuisine_type'].value_counts()
                axes[0, 1].pie(cuisine_counts.values, labels=cuisine_counts.index, 
                              autopct='%1.1f%%', colors=sns.color_palette("husl", len(cuisine_counts)))
                axes[0, 1].set_title('Restaurants by Cuisine', fontweight='bold')
        
        # Riders rating distribution
        if 'riders' in self.reference_data:
            riders_df = self.reference_data['riders']
            if 'rating' in riders_df.columns:
                # Filter out corrupted values and convert to numeric
                rating_series = pd.to_numeric(riders_df['rating'], errors='coerce').dropna()
                rating_series.hist(bins=10, ax=axes[1, 0], color='#3498DB', edgecolor='white')
                axes[1, 0].set_xlabel('Rating')
                axes[1, 0].set_ylabel('Frequency')
                axes[1, 0].set_title('Rider Rating Distribution', fontweight='bold')
                axes[1, 0].set_xlim(0.0, 5.0)  # Set explicit x-axis limits, normally we won't need this
                axes[1, 0].tick_params(axis='x', rotation=0)
            
            if 'total_deliveries' in riders_df.columns:
                delivery_series = pd.to_numeric(riders_df['total_deliveries'], errors='coerce').dropna()
                delivery_series.hist(bins=10, ax=axes[1, 1], 
                                     color='#2ECC71', edgecolor='white')
                axes[1, 1].set_xlabel('Total Deliveries')
                axes[1, 1].set_ylabel('Frequency')
                axes[1, 1].set_title('Rider Delivery Count Distribution', fontweight='bold')
                axes[1, 1].xaxis.set_major_locator(MaxNLocator(integer=True, nbins=10))
                axes[1, 1].tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        
        if save:
            plt.savefig(self.output_dir / 'reference_data_stats.png', dpi=150)
            print(f"Saved: {self.output_dir / 'reference_data_stats.png'}")
        plt.show()
    
    # =========================================================================
    # Statistics Report
    # =========================================================================
    
    def generate_stats_report(self) -> Dict[str, Any]:
        """Generate comprehensive statistics report"""
        order_df = self._prepare_order_df()
        rider_df = self._prepare_rider_df()
        
        report = {
            'summary': {
                'total_order_events': len(self.order_events),
                'total_rider_events': len(self.rider_events),
                'reference_tables': list(self.reference_data.keys())
            },
            'order_events': {},
            'rider_events': {},
            'reference_data': {},
            'errors': {}
        }
        
        # Order stats
        if not order_df.empty:
            report['order_events'] = {
                'event_types': order_df['event_type'].value_counts().to_dict() if 'event_type' in order_df else {},
                'regions': order_df['region'].value_counts().to_dict() if 'region' in order_df else {},
                'payment_methods': order_df['payment_method'].value_counts().to_dict() if 'payment_method' in order_df else {},
                'order_value_stats': {
                    'mean': order_df['order_value'].mean() if 'order_value' in order_df else None,
                    'median': order_df['order_value'].median() if 'order_value' in order_df else None,
                    'std': order_df['order_value'].std() if 'order_value' in order_df else None,
                    'min': order_df['order_value'].min() if 'order_value' in order_df else None,
                    'max': order_df['order_value'].max() if 'order_value' in order_df else None
                }
            }
        
        # Rider stats
        if not rider_df.empty:
            report['rider_events'] = {
                'event_types': rider_df['event_type'].value_counts().to_dict() if 'event_type' in rider_df else {},
                'statuses': rider_df['status'].value_counts().to_dict() if 'status' in rider_df else {},
                'unique_riders': rider_df['rider_id'].nunique() if 'rider_id' in rider_df else 0
            }
        
        # Reference data stats
        for name, df in self.reference_data.items():
            report['reference_data'][name] = {
                'row_count': len(df),
                'columns': list(df.columns),
                'null_counts': df.isnull().sum().to_dict()
            }
        
        # Error stats - malformed JSON entries are now raw strings, not dicts
        report['errors'] = {
            'order_malformed': sum(1 for e in self.order_events if isinstance(e, str)),
            'order_null_values': sum(1 for e in self.order_events if isinstance(e, dict) and any(v is None for k, v in e.items() if k != 'rider_id')),
            'rider_malformed': sum(1 for e in self.rider_events if isinstance(e, str)),
            'rider_null_values': sum(1 for e in self.rider_events if isinstance(e, dict) and any(v is None for v in e.values())),
        }
        
        return report
    
    def print_stats_report(self) -> None:
        """Print formatted statistics report"""
        report = self.generate_stats_report()
        
        print("\n" + "=" * 70)
        print("SYNTHETIC DATA STATISTICS REPORT")
        print("=" * 70)
        
        print("\n SUMMARY")
        print("-" * 40)
        print(f"  Total Order Events: {report['summary']['total_order_events']:,}")
        print(f"  Total Rider Events: {report['summary']['total_rider_events']:,}")
        print(f"  Reference Tables: {', '.join(report['summary']['reference_tables'])}")
        
        if report['order_events']:
            print("\n ORDER EVENTS")
            print("-" * 40)
            print("  Event Types:")
            for event_type, count in report['order_events'].get('event_types', {}).items():
                print(f"    - {event_type}: {count:,}")
            
            print("  Regions:")
            for region, count in report['order_events'].get('regions', {}).items():
                print(f"    - {region}: {count:,}")
            
            if report['order_events'].get('order_value_stats'):
                stats = report['order_events']['order_value_stats']
                print("  Order Value:")
                print(f"    - Mean: INR{stats['mean']:.2f}" if stats['mean'] else "    - Mean: N/A")
                print(f"    - Median: INR{stats['median']:.2f}" if stats['median'] else "    - Median: N/A")
                print(f"    - Std Dev: INR{stats['std']:.2f}" if stats['std'] else "    - Std Dev: N/A")
        
        if report['rider_events']:
            print("\n  RIDER EVENTS")
            print("-" * 40)
            print("  Event Types:")
            for event_type, count in report['rider_events'].get('event_types', {}).items():
                print(f"    - {event_type}: {count:,}")
            print(f"  Unique Riders: {report['rider_events'].get('unique_riders', 0):,}")
        
        print("\n  INJECTED ERRORS")
        print("-" * 40)
        for error_type, count in report['errors'].items():
            pct = (count / max(report['summary']['total_order_events'], 1)) * 100
            print(f"  - {error_type}: {count:,} ({pct:.2f}%)")
        
        print("\n" + "=" * 70)
    
    def generate_all_visualizations(self, save: bool = True) -> None:
        """Generate all available visualizations"""
        print("\nGenerating visualizations...")
        print("-" * 40)
        
        self.plot_order_volume_over_time(save)
        self.plot_order_event_types(save)
        self.plot_order_by_region(save)
        self.plot_order_value_distribution(save)
        self.plot_rider_event_types(save)
        self.plot_rider_locations(save)
        self.plot_rider_status_distribution(save)
        self.plot_error_analysis(save)
        self.plot_reference_data_stats(save)
        
        print(f"\n All visualizations saved to: {self.output_dir.absolute()}")


def main() -> None:
    """Main entry point for visualization"""
    parser = argparse.ArgumentParser(description='Visualize and analyze synthetic delivery platform data')
    parser.add_argument('--data-dir', type=str, default='data', help='Data directory path')
    parser.add_argument('--orders-file', type=str, default='simulation_1h_orders.json', 
                        help='Orders JSON file name')
    parser.add_argument('--riders-file', type=str, default='simulation_1h_riders.json',
                        help='Riders JSON file name')
    parser.add_argument('--no-save', action='store_true', help='Do not save visualizations')
    parser.add_argument('--stats-only', action='store_true', help='Only print statistics')
    
    args = parser.parse_args()
    
    # Initialize visualizer
    viz = DataVisualizer(data_dir=args.data_dir)
    
    # Load data
    print("Loading data...")
    viz.load_events(args.orders_file, args.riders_file)
    viz.load_reference_data()
    
    if not viz.order_events and not viz.rider_events:
        print("\nNo event data found. Run main.py first to generate data.")
        print("Usage: python main.py")
        sys.exit(1)
    
    # Print statistics
    viz.print_stats_report()
    
    # Generate visualizations
    if not args.stats_only:
        viz.generate_all_visualizations(save=not args.no_save)


if __name__ == "__main__":
    main()
