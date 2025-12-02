# visualization/plot_generator.py - Generate visualizations
import logging
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import os
from datetime import datetime
from typing import Dict, Any, List, Optional
from utils.helpers import time_function

logger = logging.getLogger(__name__)

class PlotGenerator:
    def __init__(self, output_dir: str = "visualizations"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Set plotting style
        plt.style.use('seaborn-v0_8-darkgrid')
        sns.set_palette("husl")
        
        # Configure font sizes
        plt.rcParams['figure.titlesize'] = 16
        plt.rcParams['axes.titlesize'] = 14
        plt.rcParams['axes.labelsize'] = 12
        plt.rcParams['xtick.labelsize'] = 10
        plt.rcParams['ytick.labelsize'] = 10
    
    @time_function
    def plot_event_type_distribution(self, df_event_types: pd.DataFrame, 
                                   top_n: int = 10) -> Optional[str]:
        """Plot event type distribution"""
        if df_event_types.empty:
            logger.warning("No event type data to plot")
            return None
        
        try:
            # Take top N event types
            df_plot = df_event_types.head(top_n).copy()
            
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
            fig.suptitle('GitHub Event Type Distribution', fontweight='bold')
            
            # Bar chart
            bars = ax1.bar(df_plot['event_type'], df_plot['count'], 
                          color=plt.cm.Set3(range(len(df_plot))))
            ax1.set_title(f'Top {len(df_plot)} Event Types')
            ax1.set_xlabel('Event Type')
            ax1.set_ylabel('Count')
            ax1.tick_params(axis='x', rotation=45)
            ax1.grid(True, alpha=0.3, axis='y')
            
            # Add counts on bars
            for bar, count in zip(bars, df_plot['count']):
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2, height + max(df_plot['count'])*0.01,
                        f'{count:,}', ha='center', va='bottom', fontsize=9)
            
            # Pie chart
            ax2.pie(df_plot['count'], labels=df_plot['event_type'],
                   autopct='%1.1f%%', startangle=90,
                   colors=plt.cm.Set3(range(len(df_plot))))
            ax2.set_title('Percentage Distribution')
            
            plt.tight_layout()
            
            # Save figure
            filename = f"{self.output_dir}/event_type_distribution.png"
            plt.savefig(filename, dpi=150, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Saved event type plot: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error plotting event types: {e}")
            return None
    
    @time_function
    def plot_top_repositories(self, df_top_repos: pd.DataFrame, 
                            top_n: int = 15) -> Optional[str]:
        """Plot top repositories"""
        if df_top_repos.empty:
            logger.warning("No repository data to plot")
            return None
        
        try:
            # Take top N repositories
            df_plot = df_top_repos.head(top_n).copy()
            
            # Truncate long repository names for display
            display_names = []
            for name in df_plot['repo_name']:
                if len(name) > 40:
                    display_names.append(name[:37] + '...')
                else:
                    display_names.append(name)
            
            plt.figure(figsize=(12, 8))
            
            bars = plt.barh(range(len(df_plot)), df_plot['total_events'],
                           color=plt.cm.viridis(range(len(df_plot))))
            plt.yticks(range(len(df_plot)), display_names)
            plt.title(f'Top {len(df_plot)} Most Active Repositories', fontweight='bold')
            plt.xlabel('Number of Events')
            plt.grid(True, alpha=0.3, axis='x')
            
            # Add event counts
            for i, (_, row) in enumerate(df_plot.iterrows()):
                plt.text(row['total_events'] + max(df_plot['total_events'])*0.01, 
                        i, f'{row["total_events"]:,}', va='center', fontsize=9)
            
            plt.tight_layout()
            
            # Save figure
            filename = f"{self.output_dir}/top_repositories.png"
            plt.savefig(filename, dpi=150, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Saved top repositories plot: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error plotting top repositories: {e}")
            return None
    
    @time_function
    def plot_temporal_patterns(self, temporal_data: Dict[str, pd.DataFrame]) -> Optional[str]:
        """Plot temporal patterns"""
        if not temporal_data:
            logger.warning("No temporal data to plot")
            return None
        
        try:
            fig, axes = plt.subplots(2, 2, figsize=(16, 10))
            fig.suptitle('Temporal Patterns in GitHub Activity', fontweight='bold')
            
            # Hourly activity
            if 'hourly' in temporal_data and not temporal_data['hourly'].empty:
                df_hourly = temporal_data['hourly']
                axes[0, 0].plot(df_hourly['hour_of_day'], df_hourly['event_count'], 
                               marker='o', linewidth=2, color='steelblue')
                axes[0, 0].fill_between(df_hourly['hour_of_day'], df_hourly['event_count'],
                                       alpha=0.3, color='steelblue')
                axes[0, 0].set_title('Activity by Hour of Day (UTC)', fontweight='bold')
                axes[0, 0].set_xlabel('Hour')
                axes[0, 0].set_ylabel('Number of Events')
                axes[0, 0].grid(True, alpha=0.3)
                axes[0, 0].set_xticks(range(0, 24, 3))
            
            # Daily activity
            if 'daily' in temporal_data and not temporal_data['daily'].empty:
                df_daily = temporal_data['daily']
                colors = plt.cm.Set2(range(len(df_daily)))
                axes[0, 1].bar(df_daily['day_of_week'], df_daily['event_count'],
                              color=colors)
                axes[0, 1].set_title('Activity by Day of Week', fontweight='bold')
                axes[0, 1].set_xlabel('Day')
                axes[0, 1].set_ylabel('Number of Events')
                axes[0, 1].tick_params(axis='x', rotation=45)
                axes[0, 1].grid(True, alpha=0.3, axis='y')
                
                # Add counts on bars
                for i, (_, row) in enumerate(df_daily.iterrows()):
                    axes[0, 1].text(i, row['event_count'] + max(df_daily['event_count'])*0.01,
                                   f'{row["event_count"]:,}', ha='center', va='bottom', fontsize=9)
            
            # Monthly trends
            if 'monthly' in temporal_data and not temporal_data['monthly'].empty:
                df_monthly = temporal_data['monthly']
                axes[1, 0].plot(range(len(df_monthly)), df_monthly['event_count'],
                               marker='s', linewidth=2, color='green')
                axes[1, 0].set_title('Monthly Activity Trend', fontweight='bold')
                axes[1, 0].set_xlabel('Time Period')
                axes[1, 0].set_ylabel('Number of Events')
                axes[1, 0].grid(True, alpha=0.3)
                
                # Set x-ticks for months
                if len(df_monthly) <= 12:
                    axes[1, 0].set_xticks(range(len(df_monthly)))
                    axes[1, 0].set_xticklabels(df_monthly['month'], rotation=45)
            
            # Unique contributors over time
            if 'monthly' in temporal_data and not temporal_data['monthly'].empty:
                df_monthly = temporal_data['monthly']
                axes[1, 1].plot(range(len(df_monthly)), df_monthly['unique_actors'],
                               marker='^', linewidth=2, color='orange', label='Unique Contributors')
                axes[1, 1].plot(range(len(df_monthly)), df_monthly['unique_repos'],
                               marker='d', linewidth=2, color='purple', label='Unique Repositories')
                axes[1, 1].set_title('Community Growth', fontweight='bold')
                axes[1, 1].set_xlabel('Time Period')
                axes[1, 1].set_ylabel('Count')
                axes[1, 1].grid(True, alpha=0.3)
                axes[1, 1].legend()
                
                if len(df_monthly) <= 12:
                    axes[1, 1].set_xticks(range(len(df_monthly)))
                    axes[1, 1].set_xticklabels(df_monthly['month'], rotation=45)
            
            plt.tight_layout()
            
            # Save figure
            filename = f"{self.output_dir}/temporal_patterns.png"
            plt.savefig(filename, dpi=150, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Saved temporal patterns plot: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error plotting temporal patterns: {e}")
            return None
    
    @time_function
    def plot_repository_health(self, df_health: pd.DataFrame) -> Optional[str]:
        """Plot repository health metrics"""
        if df_health.empty:
            logger.warning("No repository health data to plot")
            return None
        
        try:
            # Take top 10 repositories for readability
            df_plot = df_health.head(10).copy()
            
            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            fig.suptitle('Repository Health Analysis', fontweight='bold')
            
            # Truncate repository names
            display_names = [name[:30] + '...' if len(name) > 30 else name 
                           for name in df_plot['repo_name']]
            
            # Plot 1: Total events
            axes[0, 0].bar(display_names, df_plot['total_events'], color='skyblue')
            axes[0, 0].set_title('Total Events', fontweight='bold')
            axes[0, 0].set_ylabel('Event Count')
            axes[0, 0].tick_params(axis='x', rotation=45)
            axes[0, 0].grid(True, alpha=0.3, axis='y')
            
            # Plot 2: Unique contributors
            axes[0, 1].bar(display_names, df_plot['unique_contributors'], color='lightgreen')
            axes[0, 1].set_title('Unique Contributors', fontweight='bold')
            axes[0, 1].set_ylabel('Contributor Count')
            axes[0, 1].tick_params(axis='x', rotation=45)
            axes[0, 1].grid(True, alpha=0.3, axis='y')
            
            # Plot 3: Activity diversity
            axes[1, 0].bar(display_names, df_plot['activity_diversity'], color='orange')
            axes[1, 0].set_title('Activity Diversity', fontweight='bold')
            axes[1, 0].set_ylabel('Number of Event Types')
            axes[1, 0].tick_params(axis='x', rotation=45)
            axes[1, 0].grid(True, alpha=0.3, axis='y')
            
            # Plot 4: Push vs collaboration percentage
            width = 0.35
            x = range(len(df_plot))
            axes[1, 1].bar([i - width/2 for i in x], df_plot['push_percentage'], 
                          width, label='Code Changes', color='steelblue')
            axes[1, 1].bar([i + width/2 for i in x], df_plot['collaboration_percentage'], 
                          width, label='Collaboration', color='coral')
            axes[1, 1].set_title('Activity Composition', fontweight='bold')
            axes[1, 1].set_ylabel('Percentage (%)')
            axes[1, 1].set_xticks(x)
            axes[1, 1].set_xticklabels(display_names, rotation=45)
            axes[1, 1].legend()
            axes[1, 1].grid(True, alpha=0.3, axis='y')
            
            plt.tight_layout()
            
            # Save figure
            filename = f"{self.output_dir}/repository_health.png"
            plt.savefig(filename, dpi=150, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Saved repository health plot: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error plotting repository health: {e}")
            return None
    
    @time_function
    def create_summary_dashboard(self, analysis_results: Dict[str, Any]) -> Optional[str]:
        """Create a summary dashboard with key metrics"""
        logger.info("Creating summary dashboard...")
        
        try:
            fig, axes = plt.subplots(2, 3, figsize=(15, 10))
            fig.suptitle('GitHub Events Analysis Dashboard', fontsize=18, fontweight='bold')
            
            insights = analysis_results.get('insights', {})
            basic_stats = analysis_results.get('basic_statistics', {})
            
            # Metric 1: Total events
            total_events = basic_stats.get('total_events', 0)
            axes[0, 0].text(0.5, 0.5, f"{total_events:,}\nTotal Events", 
                           ha='center', va='center', fontsize=24, fontweight='bold')
            axes[0, 0].set_title('Total Events', fontweight='bold')
            axes[0, 0].axis('off')
            
            # Metric 2: Date range
            date_range = basic_stats.get('date_range', {})
            if date_range:
                start_date = date_range.get('start', 'N/A')
                end_date = date_range.get('end', 'N/A')
                if hasattr(start_date, 'strftime'):
                    start_str = start_date.strftime('%Y-%m-%d')
                    end_str = end_date.strftime('%Y-%m-%d')
                    axes[0, 1].text(0.5, 0.5, f"{start_str}\nto\n{end_str}", 
                                   ha='center', va='center', fontsize=16)
            else:
                axes[0, 1].text(0.5, 0.5, "Date range\nnot available", 
                               ha='center', va='center', fontsize=16)
            axes[0, 1].set_title('Date Range', fontweight='bold')
            axes[0, 1].axis('off')
            
            # Metric 3: Most common event
            if 'most_common_event' in insights:
                event = insights['most_common_event']
                axes[0, 2].text(0.5, 0.5, f"{event['type']}\n{event['percentage']:.1f}%", 
                               ha='center', va='center', fontsize=20, fontweight='bold')
            else:
                axes[0, 2].text(0.5, 0.5, "Event data\nnot available", 
                               ha='center', va='center', fontsize=16)
            axes[0, 2].set_title('Most Common Event', fontweight='bold')
            axes[0, 2].axis('off')
            
            # Metric 4: Most active repository
            if 'most_active_repo' in insights:
                repo = insights['most_active_repo']
                repo_name = repo['name'].split('/')[-1][:15]
                axes[1, 0].text(0.5, 0.5, f"{repo_name}\n{repo['events']:,} events", 
                               ha='center', va='center', fontsize=18, fontweight='bold')
            else:
                axes[1, 0].text(0.5, 0.5, "Repo data\nnot available", 
                               ha='center', va='center', fontsize=16)
            axes[1, 0].set_title('Most Active Repo', fontweight='bold')
            axes[1, 0].axis('off')
            
            # Metric 5: Busiest hour
            if 'busiest_hour' in insights:
                hour = insights['busiest_hour']
                axes[1, 1].text(0.5, 0.5, f"{hour['hour']:02d}:00 UTC\n{hour['event_count']:,} events", 
                               ha='center', va='center', fontsize=18, fontweight='bold')
            else:
                axes[1, 1].text(0.5, 0.5, "Hourly data\nnot available", 
                               ha='center', va='center', fontsize=16)
            axes[1, 1].set_title('Peak Activity', fontweight='bold')
            axes[1, 1].axis('off')
            
            # Metric 6: Analysis timestamp
            timestamp = datetime.now().strftime('%Y-%m-%d\n%H:%M:%S')
            axes[1, 2].text(0.5, 0.5, f"Generated:\n{timestamp}", 
                           ha='center', va='center', fontsize=16)
            axes[1, 2].set_title('Last Updated', fontweight='bold')
            axes[1, 2].axis('off')
            
            plt.tight_layout()
            
            # Save figure
            filename = f"{self.output_dir}/summary_dashboard.png"
            plt.savefig(filename, dpi=150, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Saved summary dashboard: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error creating summary dashboard: {e}")
            return None
        
    @time_function
    def plot_package_comparison(self, df_comparison: pd.DataFrame) -> Optional[str]:
        """Plot comparative analysis between packages"""
        if df_comparison.empty or len(df_comparison) < 3:
            logger.warning("Not enough package data for comparison")
            return None
        
        try:
            fig, axes = plt.subplots(2, 2, figsize=(16, 12))
            fig.suptitle('Comparative Analysis of Python Data Science Packages', 
                        fontsize=18, fontweight='bold')
            
            # Plot 1: Total activity
            df_sorted = df_comparison.sort_values('total_events', ascending=True)
            bars1 = axes[0, 0].barh(df_sorted['package_name'], df_sorted['total_events'],
                                color=plt.cm.Blues(range(len(df_sorted))))
            axes[0, 0].set_title('Total GitHub Activity (Last 90 Days)', fontweight='bold')
            axes[0, 0].set_xlabel('Number of Events')
            axes[0, 0].grid(True, alpha=0.3, axis='x')
            
            # Plot 2: Activity composition
            categories = ['push_events', 'star_events', 'issue_events', 'pr_events']
            colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
            
            bottom = [0] * len(df_comparison)
            for i, (category, color) in enumerate(zip(categories, colors)):
                values = df_comparison[category].values
                axes[0, 1].bar(df_comparison['package_name'], values, bottom=bottom, 
                            label=category.replace('_', ' ').title(), color=color)
                bottom = [b + v for b, v in zip(bottom, values)]
            
            axes[0, 1].set_title('Activity Composition by Type', fontweight='bold')
            axes[0, 1].set_ylabel('Number of Events')
            axes[0, 1].tick_params(axis='x', rotation=45)
            axes[0, 1].legend()
            axes[0, 1].grid(True, alpha=0.3, axis='y')
            
            # Plot 3: Recent activity (last 30 days)
            df_recent = df_comparison.sort_values('events_last_30_days', ascending=True)
            bars3 = axes[1, 0].barh(df_recent['package_name'], df_recent['events_last_30_days'],
                                color=plt.cm.Greens(range(len(df_recent))))
            axes[1, 0].set_title('Recent Activity (Last 30 Days)', fontweight='bold')
            axes[1, 0].set_xlabel('Number of Events')
            axes[1, 0].grid(True, alpha=0.3, axis='x')
            
            # Plot 4: Community size vs activity
            scatter = axes[1, 1].scatter(df_comparison['unique_contributors'], 
                                        df_comparison['events_per_day'],
                                        s=df_comparison['total_events']/100,  # Size by total events
                                        c=range(len(df_comparison)), cmap='viridis',
                                        alpha=0.7)
            
            # Add package labels
            for i, row in df_comparison.iterrows():
                axes[1, 1].text(row['unique_contributors'], row['events_per_day'],
                            row['package_name'], fontsize=9, alpha=0.8)
            
            axes[1, 1].set_title('Community Size vs Activity Velocity', fontweight='bold')
            axes[1, 1].set_xlabel('Unique Contributors')
            axes[1, 1].set_ylabel('Events Per Day')
            axes[1, 1].grid(True, alpha=0.3)
            
            plt.tight_layout()
            
            filename = f"{self.output_dir}/package_comparison.png"
            plt.savefig(filename, dpi=150, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Saved package comparison plot: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error plotting package comparison: {e}")
            return None
        
    @time_function
    def generate_all_plots(self, analysis_results: Dict[str, Any]) -> Dict[str, str]:
        """Generate all plots from analysis results"""
        logger.info("Generating all visualizations...")
        
        plot_files = {}
        
        try:
            # Generate individual plots
            if 'event_types' in analysis_results:
                plot_files['event_types'] = self.plot_event_type_distribution(
                    analysis_results['event_types']
                )
            
            if 'top_repositories' in analysis_results:
                plot_files['top_repos'] = self.plot_top_repositories(
                    analysis_results['top_repositories']
                )
            
            if 'temporal_patterns' in analysis_results:
                plot_files['temporal'] = self.plot_temporal_patterns(
                    analysis_results['temporal_patterns']
                )
            
            if 'repository_health' in analysis_results:
                plot_files['health'] = self.plot_repository_health(
                    analysis_results['repository_health']
                )

            if 'package_comparison' in analysis_results:
                plot_files['package_comparison'] = self.plot_package_comparison(
                    analysis_results['package_comparison']
                )
            
            # Generate summary dashboard
            plot_files['dashboard'] = self.create_summary_dashboard(analysis_results)
            
            # Create a summary file
            self._create_plot_summary(plot_files)
            
            logger.info(f"Generated {len([f for f in plot_files.values() if f])} plots")
            return plot_files
            
        except Exception as e:
            logger.error(f"Error generating plots: {e}")
            return {}
    
    def _create_plot_summary(self, plot_files: Dict[str, str]):
        """Create a summary file listing all generated plots"""
        try:
            summary_file = f"{self.output_dir}/plots_summary.txt"
          
            with open(summary_file, 'w') as f:
                f.write("GENERATED VISUALIZATIONS SUMMARY\n")

                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
              
                for plot_name, plot_path in plot_files.items():
                    if plot_path:
                        f.write(f"{plot_name.replace('_', ' ').title()}: {plot_path}\n")
              
          
            logger.info(f"Created plot summary: {summary_file}")
          
        except Exception as e:
            logger.error(f"Error creating plot summary: {e}")

def main():
    """Main visualization function"""
    logger.info("Starting visualization generation...")
    
    try:
        # For demo, run analysis first
        from analysis.data_analyzer import DataAnalyzer
        
        analyzer = DataAnalyzer()
        if not analyzer.connect():
            return {}
        
        analysis_results = {
            'basic_statistics': analyzer.get_basic_statistics(),
            'event_types': analyzer.analyze_event_types(),
            'top_repositories': analyzer.analyze_top_repositories(),
            'temporal_patterns': analyzer.analyze_temporal_patterns(),
            'repository_health': analyzer.analyze_repository_health(),
            'insights': analyzer.generate_insights(),
            'package_comparison': analyzer.compare_packages([
                'pandas', 'numpy', 'matplotlib', 'pytorch', 'tensorflow'
            ])
        }
        
        analyzer.close()
        
        # Generate plots
        generator = PlotGenerator()
        plot_files = generator.generate_all_plots(analysis_results)
        
        # Print summary
        print("Successfully created visualizations!\n")
        print(f"Generated plots saved to: {generator.output_dir}/")
        
        for plot_name, plot_path in plot_files.items():
            if plot_path:
                print(f"  • {plot_name}: {os.path.basename(plot_path)}")
        
        logger.info("Visualization generation complete")
        return plot_files
        
    except Exception as e:
        logger.error(f"Visualization generation failed: {e}")
        return {}

if __name__ == "__main__":
    # Setup logging
    from utils.logger import setup_logging
    setup_logging()
    
    main()