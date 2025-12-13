"""
Real-Time Stock Portfolio Analytics Dashboard
Interactive Streamlit dashboard for stock portfolio data visualization
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine
import os
from datetime import datetime, timedelta
import json

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="Real-Time Stock Portfolio Analytics",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_db_connection():
    """Create database connection using environment variables"""
    try:
        host = os.getenv('POSTGRES_HOST', 'postgres')
        port = os.getenv('POSTGRES_PORT', '5432')
        database = os.getenv('POSTGRES_DB', 'stock_portfolio')
        user = os.getenv('POSTGRES_USER', 'postgres')
        password = os.getenv('POSTGRES_PASSWORD', 'postgres')
        
        connection_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        st.error(f"Database connection error: {str(e)}")
        return None


@st.cache_data(ttl=60)
def load_data_from_db(table_name='stock_trades_integrated'):
    """
    Fallback: Load data from PostgreSQL database.
    Uses stock_trades_integrated table (pre-streaming, decoded data).
    This is a reliable fallback if streaming stage hasn't completed yet.
    """
    try:
        engine = get_db_connection()
        if engine:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, engine)
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df
        return None
    except Exception as e:
        st.warning(f"Could not load from database: {str(e)}")
        return None


@st.cache_data(ttl=60)
def load_data_from_csv():
    """
    Primary: Load data from CSV files (post-streaming output).
    This reflects the actual pipeline output after Kafka streaming.
    """
    try:
        output_dir = '/opt/airflow/output' if os.path.exists('/opt/airflow/output') else 'output'
        
        # Priority 1: FINAL_STOCKS_DECODED.csv (post-streaming, decoded by visualization task)
        decoded_path = os.path.join(output_dir, 'FINAL_STOCKS_DECODED.csv')
        if os.path.exists(decoded_path):
            df = pd.read_csv(decoded_path)
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df
        
        # Priority 2: FULL_STOCKS.csv (may be post-streaming)
        full_stocks_path = os.path.join(output_dir, 'FULL_STOCKS.csv')
        if os.path.exists(full_stocks_path):
            df = pd.read_csv(full_stocks_path)
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df
        
        return None
    except Exception as e:
        st.warning(f"Could not load from CSV: {str(e)}")
        return None


def load_metadata():
    """Load metadata about the visualization data"""
    try:
        output_dir = '/opt/airflow/output' if os.path.exists('/opt/airflow/output') else 'output'
        metadata_path = os.path.join(output_dir, 'visualization_metadata.json')
        
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                return json.load(f)
    except:
        pass
    return None


def format_number(num):
    """Format large numbers for display"""
    if num >= 1_000_000:
        return f"${num/1_000_000:.2f}M"
    elif num >= 1_000:
        return f"${num/1_000:.2f}K"
    else:
        return f"${num:.2f}"


def export_chart_to_image(fig, filename):
    """Export plotly figure to PNG"""
    try:
        fig.write_image(filename)
        return True
    except:
        return False


# ============================================================================
# MAIN DASHBOARD
# ============================================================================

def main():
    # Dashboard Header
    st.title("📈 Real-Time Stock Portfolio Analytics")
    st.markdown("### Interactive Dashboard for Stock Trading Analysis")
    st.markdown("---")
    
    # Load Data
    with st.spinner("Loading data..."):
        # Try CSV first (post-streaming, most realistic), fallback to database (pre-streaming)
        df = load_data_from_csv()
        if df is None or len(df) == 0:
            st.info("Post-streaming data not found, loading from database (pre-streaming)...")
            df = load_data_from_db()
        
        if df is None or len(df) == 0:
            st.error("No data available. Please run the pipeline first.")
            st.stop()
    
    # Load metadata
    metadata = load_metadata()
    
    # ========================================================================
    # SIDEBAR FILTERS
    # ========================================================================
    
    st.sidebar.header("📊 Filters")
    
    # Date Range Filter
    if 'timestamp' in df.columns:
        min_date = df['timestamp'].min().date()
        max_date = df['timestamp'].max().date()
        
        date_range = st.sidebar.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        if len(date_range) == 2:
            start_date, end_date = date_range
            df = df[(df['timestamp'].dt.date >= start_date) & (df['timestamp'].dt.date <= end_date)]
    
    # Stock Ticker Filter
    if 'stock_ticker' in df.columns:
        all_tickers = ['All'] + sorted(df['stock_ticker'].unique().tolist())
        selected_tickers = st.sidebar.multiselect(
            "Stock Ticker",
            options=all_tickers,
            default=['All']
        )
        
        if 'All' not in selected_tickers and selected_tickers:
            df = df[df['stock_ticker'].isin(selected_tickers)]
    
    # Sector Filter
    if 'stock_sector' in df.columns:
        all_sectors = ['All'] + sorted(df['stock_sector'].unique().tolist())
        selected_sectors = st.sidebar.multiselect(
            "Sector",
            options=all_sectors,
            default=['All']
        )
        
        if 'All' not in selected_sectors and selected_sectors:
            df = df[df['stock_sector'].isin(selected_sectors)]
    
    # Customer Type Filter
    if 'customer_account_type' in df.columns:
        all_customer_types = ['All'] + sorted(df['customer_account_type'].unique().tolist())
        selected_customer_type = st.sidebar.selectbox(
            "Customer Type",
            options=all_customer_types
        )
        
        if selected_customer_type != 'All':
            df = df[df['customer_account_type'] == selected_customer_type]
    
    # Transaction Type Filter
    if 'transaction_type' in df.columns:
        transaction_types = ['All'] + sorted(df['transaction_type'].unique().tolist())
        selected_transaction = st.sidebar.selectbox(
            "Transaction Type",
            options=transaction_types
        )
        
        if selected_transaction != 'All':
            df = df[df['transaction_type'] == selected_transaction]
    
    st.sidebar.markdown("---")
    
    # Refresh Button
    if st.sidebar.button("🔄 Refresh Data", type="primary"):
        st.cache_data.clear()
        st.rerun()
    
    # Export Options
    st.sidebar.markdown("---")
    st.sidebar.subheader("📥 Export Options")
    
    csv = df.to_csv(index=False).encode('utf-8')
    st.sidebar.download_button(
        label="Download Filtered Data (CSV)",
        data=csv,
        file_name=f"stock_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )
    
    # ========================================================================
    # KPI METRICS
    # ========================================================================
    
    st.subheader("📊 Portfolio Performance Metrics")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        total_transactions = len(df)
        st.metric(
            label="Total Transactions",
            value=f"{total_transactions:,}"
        )
    
    with col2:
        if 'total_trade_amount' in df.columns:
            total_volume = df['total_trade_amount'].sum()
            st.metric(
                label="Total Trade Volume",
                value=format_number(total_volume)
            )
    
    with col3:
        if 'stock_ticker' in df.columns:
            unique_stocks = df['stock_ticker'].nunique()
            st.metric(
                label="Unique Stocks",
                value=f"{unique_stocks}"
            )
    
    with col4:
        if 'customer_id' in df.columns:
            unique_customers = df['customer_id'].nunique()
            st.metric(
                label="Active Customers",
                value=f"{unique_customers}"
            )
    
    with col5:
        if 'stock_price' in df.columns:
            avg_stock_price = df['stock_price'].mean()
            st.metric(
                label="Avg Stock Price",
                value=f"${avg_stock_price:.2f}"
            )
    
    st.markdown("---")
    
    # ========================================================================
    # CORE VISUALIZATIONS (MANDATORY - 5 charts)
    # ========================================================================
    
    st.subheader("📈 Core Analytics")
    
    # Create tabs for better organization
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Volume by Ticker",
        "💹 Price Trends",
        "🔄 Buy vs Sell",
        "📅 Weekly Activity",
        "👥 Customer Distribution"
    ])
    
    # ---- TAB 1: Trading Volume by Stock Ticker ----
    with tab1:
        st.markdown("#### Trading Volume by Stock Ticker")
        
        if 'stock_ticker' in df.columns and 'quantity' in df.columns:
            volume_data = df.groupby('stock_ticker')['quantity'].sum().reset_index()
            volume_data = volume_data.sort_values('quantity', ascending=False)
            
            fig1 = px.bar(
                volume_data,
                x='stock_ticker',
                y='quantity',
                title='Total Trading Volume by Stock Ticker',
                labels={'quantity': 'Total Volume', 'stock_ticker': 'Stock Ticker'},
                color='quantity',
                color_continuous_scale='Blues'
            )
            fig1.update_layout(height=500, showlegend=False)
            st.plotly_chart(fig1, use_container_width=True)
            
            # Show top performers
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Top 5 by Volume**")
                st.dataframe(volume_data.head(5), hide_index=True)
            with col2:
                st.markdown("**Bottom 5 by Volume**")
                st.dataframe(volume_data.tail(5), hide_index=True)
    
    # ---- TAB 2: Stock Price Trends by Sector ----
    with tab2:
        st.markdown("#### Stock Price Trends by Sector")
        
        if 'stock_sector' in df.columns and 'timestamp' in df.columns and 'stock_price' in df.columns:
            # Aggregate by date and sector
            price_trends = df.groupby([df['timestamp'].dt.date, 'stock_sector'])['stock_price'].mean().reset_index()
            price_trends.columns = ['date', 'stock_sector', 'avg_price']
            
            fig2 = px.line(
                price_trends,
                x='date',
                y='avg_price',
                color='stock_sector',
                title='Average Stock Price Trends by Sector Over Time',
                labels={'avg_price': 'Average Price ($)', 'date': 'Date', 'stock_sector': 'Sector'}
            )
            fig2.update_layout(height=500)
            st.plotly_chart(fig2, use_container_width=True)
            
            # Sector statistics
            sector_stats = df.groupby('stock_sector')['stock_price'].agg(['mean', 'min', 'max', 'std']).reset_index()
            sector_stats.columns = ['Sector', 'Avg Price', 'Min Price', 'Max Price', 'Std Dev']
            st.dataframe(sector_stats, hide_index=True)
    
    # ---- TAB 3: Buy vs Sell Transactions ----
    with tab3:
        st.markdown("#### Buy vs Sell Transaction Analysis")
        
        if 'transaction_type' in df.columns:
            col1, col2 = st.columns(2)
            
            with col1:
                # Pie chart
                trans_counts = df['transaction_type'].value_counts().reset_index()
                trans_counts.columns = ['transaction_type', 'count']
                
                fig3a = px.pie(
                    trans_counts,
                    names='transaction_type',
                    values='count',
                    title='Transaction Distribution',
                    color_discrete_sequence=['#00CC96', '#EF553B']
                )
                st.plotly_chart(fig3a, use_container_width=True)
            
            with col2:
                # Bar chart with amounts
                if 'total_trade_amount' in df.columns:
                    trans_amounts = df.groupby('transaction_type')['total_trade_amount'].sum().reset_index()
                    
                    fig3b = px.bar(
                        trans_amounts,
                        x='transaction_type',
                        y='total_trade_amount',
                        title='Total Trade Amount by Transaction Type',
                        labels={'total_trade_amount': 'Total Amount ($)', 'transaction_type': 'Type'},
                        color='transaction_type',
                        color_discrete_sequence=['#00CC96', '#EF553B']
                    )
                    st.plotly_chart(fig3b, use_container_width=True)
            
            # Summary statistics
            st.markdown("**Transaction Summary**")
            summary_df = df.groupby('transaction_type').agg({
                'transaction_id': 'count',
                'quantity': 'sum',
                'total_trade_amount': 'sum'
            }).reset_index()
            summary_df.columns = ['Type', 'Count', 'Total Quantity', 'Total Amount']
            st.dataframe(summary_df, hide_index=True)
    
    # ---- TAB 4: Trading Activity by Day of Week ----
    with tab4:
        st.markdown("#### Trading Activity by Day of Week")
        
        if 'day_name' in df.columns:
            # Define day order
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            
            day_activity = df.groupby('day_name').agg({
                'transaction_id': 'count',
                'quantity': 'sum',
                'total_trade_amount': 'sum'
            }).reset_index()
            day_activity.columns = ['day_name', 'transaction_count', 'total_volume', 'total_amount']
            
            # Sort by day order
            day_activity['day_name'] = pd.Categorical(day_activity['day_name'], categories=day_order, ordered=True)
            day_activity = day_activity.sort_values('day_name')
            
            fig4 = go.Figure()
            fig4.add_trace(go.Bar(
                name='Transaction Count',
                x=day_activity['day_name'],
                y=day_activity['transaction_count'],
                yaxis='y',
                offsetgroup=1
            ))
            fig4.add_trace(go.Bar(
                name='Total Volume',
                x=day_activity['day_name'],
                y=day_activity['total_volume'],
                yaxis='y2',
                offsetgroup=2
            ))
            
            fig4.update_layout(
                title='Trading Activity by Day of Week',
                xaxis=dict(title='Day of Week'),
                yaxis=dict(title='Transaction Count', side='left'),
                yaxis2=dict(title='Total Volume', overlaying='y', side='right'),
                height=500,
                barmode='group'
            )
            st.plotly_chart(fig4, use_container_width=True)
            
            st.dataframe(day_activity, hide_index=True)
    
    # ---- TAB 5: Customer Transaction Distribution ----
    with tab5:
        st.markdown("#### Customer Transaction Distribution")
        
        if 'customer_id' in df.columns and 'total_trade_amount' in df.columns:
            customer_dist = df.groupby('customer_id').agg({
                'transaction_id': 'count',
                'total_trade_amount': 'sum'
            }).reset_index()
            customer_dist.columns = ['customer_id', 'transaction_count', 'total_amount']
            customer_dist = customer_dist.sort_values('total_amount', ascending=False)
            
            # Distribution histogram
            fig5 = px.histogram(
                customer_dist,
                x='total_amount',
                nbins=50,
                title='Distribution of Customer Trade Amounts',
                labels={'total_amount': 'Total Trade Amount ($)', 'count': 'Number of Customers'}
            )
            fig5.update_layout(height=400)
            st.plotly_chart(fig5, use_container_width=True)
            
            # Top 10 customers
            st.markdown("**Top 10 Customers by Trade Amount**")
            top_10 = customer_dist.head(10)
            
            fig5b = px.bar(
                top_10,
                x='customer_id',
                y='total_amount',
                title='Top 10 Customers by Total Trade Amount',
                labels={'total_amount': 'Total Amount ($)', 'customer_id': 'Customer ID'},
                color='total_amount',
                color_continuous_scale='Viridis'
            )
            fig5b.update_layout(height=400)
            st.plotly_chart(fig5b, use_container_width=True)
            
            st.dataframe(top_10, hide_index=True)
    
    st.markdown("---")
    
    # ========================================================================
    # ADVANCED VISUALIZATIONS (Choose at least 2)
    # ========================================================================
    
    st.subheader("🚀 Advanced Analytics")
    
    adv_tab1, adv_tab2, adv_tab3, adv_tab4 = st.tabs([
        "📡 Real-Time Monitor",
        "🏢 Sector Comparison",
        "🎉 Holiday Analysis",
        "💧 Liquidity Analysis"
    ])
    
    # ---- ADVANCED 1: Real-time Streaming Data Monitor ----
    with adv_tab1:
        st.markdown("#### Real-Time Streaming Data Monitor")
        
        if 'timestamp' in df.columns:
            # Show recent transactions
            st.markdown("**Recent Transactions (Last 100)**")
            recent_df = df.sort_values('timestamp', ascending=False).head(100)
            
            # Time series of recent activity
            recent_activity = recent_df.groupby(recent_df['timestamp'].dt.floor('h')).size().reset_index()
            recent_activity.columns = ['timestamp', 'count']
            
            fig_adv1 = px.line(
                recent_activity,
                x='timestamp',
                y='count',
                title='Transaction Activity (Last 100 Transactions)',
                labels={'count': 'Transaction Count', 'timestamp': 'Time'}
            )
            fig_adv1.update_layout(height=400)
            st.plotly_chart(fig_adv1, use_container_width=True)
            
            # Display recent transactions table
            display_cols = ['timestamp', 'stock_ticker', 'transaction_type', 'quantity', 'stock_price', 'total_trade_amount']
            available_cols = [col for col in display_cols if col in recent_df.columns]
            st.dataframe(recent_df[available_cols].head(20), hide_index=True)
            
            # Auto-refresh option
            auto_refresh = st.checkbox("Enable Auto-Refresh (30s)")
            if auto_refresh:
                import time
                time.sleep(30)
                st.rerun()
    
    # ---- ADVANCED 2: Sector Comparison Dashboard ----
    with adv_tab2:
        st.markdown("#### Sector Comparison Dashboard")
        
        if 'stock_sector' in df.columns:
            sector_metrics = df.groupby('stock_sector').agg({
                'transaction_id': 'count',
                'quantity': 'sum',
                'total_trade_amount': 'sum',
                'stock_price': 'mean'
            }).reset_index()
            sector_metrics.columns = ['sector', 'transactions', 'volume', 'total_amount', 'avg_price']
            
            # Grouped bar chart
            fig_adv2 = go.Figure()
            
            fig_adv2.add_trace(go.Bar(
                name='Transaction Count',
                x=sector_metrics['sector'],
                y=sector_metrics['transactions'],
                marker_color='indianred'
            ))
            fig_adv2.add_trace(go.Bar(
                name='Total Volume',
                x=sector_metrics['sector'],
                y=sector_metrics['volume'],
                marker_color='lightsalmon'
            ))
            
            fig_adv2.update_layout(
                title='Sector Comparison: Transactions vs Volume',
                xaxis_title='Sector',
                yaxis_title='Count/Volume',
                barmode='group',
                height=500
            )
            st.plotly_chart(fig_adv2, use_container_width=True)
            
            # Sector performance table
            st.markdown("**Sector Performance Metrics**")
            st.dataframe(sector_metrics, hide_index=True)
            
            # Radar chart for sector comparison
            if len(sector_metrics) >= 3:
                # Normalize metrics for radar chart
                from sklearn.preprocessing import MinMaxScaler
                scaler = MinMaxScaler()
                
                metrics_to_scale = ['transactions', 'volume', 'total_amount', 'avg_price']
                sector_metrics_scaled = sector_metrics.copy()
                sector_metrics_scaled[metrics_to_scale] = scaler.fit_transform(sector_metrics[metrics_to_scale])
                
                fig_radar = go.Figure()
                
                for _, row in sector_metrics_scaled.iterrows():
                    fig_radar.add_trace(go.Scatterpolar(
                        r=[row['transactions'], row['volume'], row['total_amount'], row['avg_price']],
                        theta=['Transactions', 'Volume', 'Total Amount', 'Avg Price'],
                        fill='toself',
                        name=row['sector']
                    ))
                
                fig_radar.update_layout(
                    polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
                    title='Sector Performance Radar (Normalized)',
                    height=500
                )
                st.plotly_chart(fig_radar, use_container_width=True)
    
    # ---- ADVANCED 3: Holiday vs Non-Holiday Trading Patterns ----
    with adv_tab3:
        st.markdown("#### Holiday vs Non-Holiday Trading Analysis")
        
        if 'is_holiday' in df.columns:
            df['holiday_status'] = df['is_holiday'].map({True: 'Holiday', False: 'Non-Holiday', 1: 'Holiday', 0: 'Non-Holiday'})
            
            holiday_comparison = df.groupby('holiday_status').agg({
                'transaction_id': 'count',
                'quantity': 'sum',
                'total_trade_amount': ['sum', 'mean']
            }).reset_index()
            holiday_comparison.columns = ['status', 'transaction_count', 'total_volume', 'total_amount', 'avg_amount']
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig_adv3a = px.bar(
                    holiday_comparison,
                    x='status',
                    y='transaction_count',
                    title='Transaction Count: Holiday vs Non-Holiday',
                    labels={'transaction_count': 'Transaction Count', 'status': 'Status'},
                    color='status',
                    color_discrete_sequence=['#FFA15A', '#19D3F3']
                )
                st.plotly_chart(fig_adv3a, use_container_width=True)
            
            with col2:
                fig_adv3b = px.bar(
                    holiday_comparison,
                    x='status',
                    y='avg_amount',
                    title='Average Trade Amount: Holiday vs Non-Holiday',
                    labels={'avg_amount': 'Average Amount ($)', 'status': 'Status'},
                    color='status',
                    color_discrete_sequence=['#FFA15A', '#19D3F3']
                )
                st.plotly_chart(fig_adv3b, use_container_width=True)
            
            st.markdown("**Comparison Summary**")
            st.dataframe(holiday_comparison, hide_index=True)
            
            # Statistical comparison
            if len(df) > 0:
                st.markdown("**Statistical Insights**")
                holiday_data = df[df['holiday_status'] == 'Holiday']['total_trade_amount']
                non_holiday_data = df[df['holiday_status'] == 'Non-Holiday']['total_trade_amount']
                
                if len(holiday_data) > 0 and len(non_holiday_data) > 0:
                    st.write(f"- Holiday average trade: ${holiday_data.mean():.2f}")
                    st.write(f"- Non-Holiday average trade: ${non_holiday_data.mean():.2f}")
                    st.write(f"- Difference: {((holiday_data.mean() - non_holiday_data.mean()) / non_holiday_data.mean() * 100):.2f}%")
    
    # ---- ADVANCED 4: Stock Liquidity Tier Analysis ----
    with adv_tab4:
        st.markdown("#### Stock Liquidity Tier Analysis")
        
        if 'stock_liquidity_tier' in df.columns and 'timestamp' in df.columns:
            # Time series by liquidity tier
            liquidity_time = df.groupby([df['timestamp'].dt.date, 'stock_liquidity_tier'])['total_trade_amount'].sum().reset_index()
            liquidity_time.columns = ['date', 'liquidity_tier', 'total_amount']
            
            fig_adv4 = px.area(
                liquidity_time,
                x='date',
                y='total_amount',
                color='liquidity_tier',
                title='Trade Amount by Liquidity Tier Over Time',
                labels={'total_amount': 'Total Amount ($)', 'date': 'Date', 'liquidity_tier': 'Liquidity Tier'}
            )
            fig_adv4.update_layout(height=500)
            st.plotly_chart(fig_adv4, use_container_width=True)
            
            # Liquidity tier distribution
            liquidity_dist = df.groupby('stock_liquidity_tier').agg({
                'transaction_id': 'count',
                'quantity': 'sum',
                'total_trade_amount': 'sum'
            }).reset_index()
            liquidity_dist.columns = ['liquidity_tier', 'transaction_count', 'total_volume', 'total_amount']
            
            st.markdown("**Liquidity Tier Breakdown**")
            st.dataframe(liquidity_dist, hide_index=True)
            
            # Sunburst chart
            if 'stock_sector' in df.columns:
                sunburst_data = df.groupby(['stock_liquidity_tier', 'stock_sector'])['total_trade_amount'].sum().reset_index()
                
                fig_sun = px.sunburst(
                    sunburst_data,
                    path=['stock_liquidity_tier', 'stock_sector'],
                    values='total_trade_amount',
                    title='Liquidity Tier and Sector Hierarchy',
                    height=500
                )
                st.plotly_chart(fig_sun, use_container_width=True)
    
    st.markdown("---")
    
    # ========================================================================
    # DATA TABLE VIEW
    # ========================================================================
    
    with st.expander("📋 View Filtered Data Table"):
        st.dataframe(df, use_container_width=True)
        st.info(f"Showing {len(df)} transactions")
    
    # ========================================================================
    # FOOTER
    # ========================================================================
    
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: gray;'>
            <p>Real-Time Stock Portfolio Analytics Dashboard | Powered by Streamlit & Plotly</p>
            <p>Last Updated: {}</p>
        </div>
        """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        unsafe_allow_html=True
    )


# ============================================================================
# RUN APPLICATION
# ============================================================================

if __name__ == "__main__":
    main()
