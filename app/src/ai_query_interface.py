"""
AI Query Interface - Interactive NL2SQL Query Tool
Streamlit app for submitting natural language queries and viewing results
"""

import streamlit as st
import pandas as pd
import json
import os
from datetime import datetime
import time

# Configure page
st.set_page_config(
    page_title="NL2SQL Query Interface",
    page_icon="🤖",
    layout="wide"
)

# Constants
AGENTS_DIR = '/agents'
QUERY_FILE = os.path.join(AGENTS_DIR, 'user_query.txt')
LOGS_FILE = os.path.join(AGENTS_DIR, 'AGENT_LOGS.JSON')
LOGS_DIR = os.path.join(AGENTS_DIR, 'logs')
RESULTS_DIR = os.path.join(AGENTS_DIR, 'generated_results')
METRICS_FILE = os.path.join(AGENTS_DIR, 'agent_metrics.json')

# Ensure directories exist
os.makedirs(AGENTS_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)

# Title and description
st.title("🤖 NL2SQL Query Interface")
st.markdown("Convert natural language questions into SQL queries using Google Gemini AI")

# Sidebar with metrics and info
with st.sidebar:
    st.header("📊 Agent Metrics")
    
    if os.path.exists(METRICS_FILE):
        try:
            with open(METRICS_FILE, 'r') as f:
                metrics = json.load(f)
            
            st.metric("Total Queries", metrics.get('total_queries', 0))
            st.metric("Successful", metrics.get('successful_queries', 0))
            st.metric("Failed", metrics.get('failed_queries', 0))
            
            if metrics.get('avg_execution_time', 0) > 0:
                st.metric("Avg Time (ms)", f"{metrics['avg_execution_time']:.2f}")
            
            if metrics.get('last_updated'):
                st.caption(f"Last updated: {metrics['last_updated'][:19]}")
        except:
            st.warning("Could not load metrics")
    else:
        st.info("No metrics available yet")
    
    st.divider()
    
    st.header("ℹ️ Info")
    st.markdown("""
    **Model**: Gemini 2.0 Flash
    
    **Table**: `stock_trades_integrated`
    
    **Features**:
    - Natural language to SQL
    - Query validation
    - Result preview
    - CSV export
    """)
    
    st.divider()
    
    # Auto-refresh toggle
    auto_refresh = st.checkbox("Auto-refresh (30s)", value=False)
    if auto_refresh:
        time.sleep(30)
        st.rerun()

# Main query input section
st.header("📝 Submit Query")

col1, col2 = st.columns([3, 1])

with col1:
    # Load current query if exists
    current_query = ""
    if os.path.exists(QUERY_FILE):
        try:
            with open(QUERY_FILE, 'r') as f:
                current_query = f.read().strip()
        except:
            pass
    
    user_query = st.text_area(
        "Enter your question in natural language:",
        value=current_query,
        height=100,
        placeholder="Example: What was the total trading volume for technology stocks?"
    )

with col2:
    st.markdown("<br>", unsafe_allow_html=True)  # Spacing
    
    if st.button("🚀 Submit Query", type="primary", use_container_width=True):
        if user_query.strip():
            try:
                with open(QUERY_FILE, 'w') as f:
                    f.write(user_query.strip())
                st.success("✅ Query submitted successfully!")
                st.info("💡 Trigger the Airflow DAG to process this query")
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"❌ Error saving query: {e}")
        else:
            st.warning("⚠️ Please enter a query")
    
    if st.button("🔄 Refresh Logs", use_container_width=True):
        st.rerun()

# Example queries
with st.expander("💡 Example Queries"):
    examples = [
        "What was the total trading volume for technology stocks?",
        "Show me the top 10 customers by trade amount",
        "What's the average stock price by sector?",
        "How many transactions happened on weekends?",
        "Compare buy vs sell transactions by account type",
        "What are the most traded stocks?",
        "Show transactions on holidays",
        "What's the total trade amount by day of week?",
        "Which sectors have the highest liquidity?",
        "Show me the trading volume trend by month"
    ]
    
    for example in examples:
        if st.button(example, key=f"ex_{examples.index(example)}"):
            with open(QUERY_FILE, 'w') as f:
                f.write(example)
            st.success(f"✅ Loaded: {example}")
            time.sleep(0.5)
            st.rerun()

st.divider()

# Query history section
st.header("📜 Query History")

# Tabs for different views
tab1, tab2, tab3 = st.tabs(["Recent Queries", "Detailed Logs", "Query Results"])

with tab1:
    st.subheader("Translation History (Simple)")
    
    if os.path.exists(LOGS_FILE):
        try:
            with open(LOGS_FILE, 'r') as f:
                logs = json.load(f)
            
            if logs:
                # Show last 10 queries
                recent_logs = logs[-10:][::-1]  # Reverse to show newest first
                
                for idx, log in enumerate(recent_logs):
                    with st.expander(f"Query {len(logs) - idx}: {log.get('user_query', 'N/A')[:80]}..."):
                        st.markdown("**User Query:**")
                        st.code(log.get('user_query', 'N/A'), language=None)
                        
                        st.markdown("**Generated SQL:**")
                        st.code(log.get('agent_response', 'N/A'), language='sql')
                        
                        # Copy button
                        st.button(
                            "📋 Copy SQL", 
                            key=f"copy_{idx}",
                            help="Click to copy SQL to clipboard"
                        )
            else:
                st.info("No query history yet. Submit a query to get started!")
        except Exception as e:
            st.error(f"Error loading logs: {e}")
    else:
        st.info("No logs file found yet")

with tab2:
    st.subheader("Detailed Execution Logs")
    
    if os.path.exists(LOGS_DIR):
        log_files = sorted(
            [f for f in os.listdir(LOGS_DIR) if f.endswith('.json')],
            reverse=True
        )
        
        if log_files:
            st.info(f"Found {len(log_files)} detailed log files")
            
            # Show last 5 detailed logs
            for log_file in log_files[:5]:
                try:
                    log_path = os.path.join(LOGS_DIR, log_file)
                    with open(log_path, 'r') as f:
                        detailed_log = json.load(f)
                    
                    timestamp = detailed_log.get('timestamp', 'Unknown')
                    query = detailed_log.get('user_query', 'N/A')
                    
                    with st.expander(f"🕐 {timestamp} - {query[:60]}..."):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown("**User Query:**")
                            st.text(query)
                            
                            if 'generated_sql' in detailed_log:
                                st.markdown("**Generated SQL:**")
                                st.code(detailed_log['generated_sql'], language='sql')
                        
                        with col2:
                            st.markdown("**Metadata:**")
                            st.json({
                                "Model": detailed_log.get('model', 'N/A'),
                                "Execution Time (ms)": detailed_log.get('execution_time_ms', 'N/A'),
                                "Status": detailed_log.get('status', 'N/A'),
                                "Enhanced Context": detailed_log.get('enhanced_context_used', 'N/A'),
                                "Cache Hit": detailed_log.get('cache_hit', 'N/A')
                            })
                            
                            if 'error' in detailed_log:
                                st.error(f"Error: {detailed_log['error']}")
                
                except Exception as e:
                    st.warning(f"Could not load {log_file}: {e}")
        else:
            st.info("No detailed logs yet")
    else:
        st.info("Logs directory not found")

with tab3:
    st.subheader("Saved Query Results")
    
    if os.path.exists(RESULTS_DIR):
        result_files = sorted(
            [f for f in os.listdir(RESULTS_DIR) if f.endswith('.csv')],
            reverse=True
        )
        
        if result_files:
            st.info(f"Found {len(result_files)} result files")
            
            selected_file = st.selectbox(
                "Select a result file to view:",
                result_files
            )
            
            if selected_file:
                try:
                    result_path = os.path.join(RESULTS_DIR, selected_file)
                    df = pd.read_csv(result_path)
                    
                    st.markdown(f"**File:** `{selected_file}`")
                    st.markdown(f"**Rows:** {len(df)} | **Columns:** {len(df.columns)}")
                    
                    # Show preview
                    st.dataframe(df.head(100), use_container_width=True)
                    
                    # Download button
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Full CSV",
                        data=csv,
                        file_name=selected_file,
                        mime="text/csv",
                        use_container_width=True
                    )
                    
                except Exception as e:
                    st.error(f"Error loading result file: {e}")
        else:
            st.info("No saved results yet")
    else:
        st.info("Results directory not found")

# Footer
st.divider()
st.caption("💡 Powered by Google Gemini 2.0 Flash | Stock Portfolio Analytics Platform")
