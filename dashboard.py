# import streamlit as st
# import pandas as pd
# import plotly.express as px
# import psycopg2

# # Style Guide Application
# st.set_page_config(page_title="MassMutual Data Pipeline Dashboard", layout="wide")
# # st.markdown("""
# # <style>
# #     [data-testid="stAppViewContainer"] { background-color: #F9FAFB; }
# #     [data-testid="stHeader"] { background-color: #F9FAFB; }
# #     .css-1aumxhk { background-color: #FFFFFF; border-radius: 6px; padding: 20px; box-shadow: 0 2px 6px rgba(0,0,0,0.1); }
# #     .stMarkdown p { color: #212121 !important; font-family: 'Roboto', sans-serif; font-size: 16px; }
# #     .stMarkdown h2 { color: #212121 !important; font-family: 'Roboto', sans-serif; font-weight: 600; }
# #     .stMarkdown h3 { color: #212121 !important; font-family: 'Roboto', sans-serif; font-weight: 600; }
# #     .stMarkdown h4 { color: #616161 !important; font-family: 'Roboto', sans-serif; font-weight: 400; }
# #     .stExpander { background-color: #E3F2FD; border-radius: 6px; padding: 10px; }
# #     .stDataFrame, .stTable { color: #212121 !important; }
# # </style>
# # """, unsafe_allow_html=True)

# # Database Connection
# def get_connection():
#     # return psycopg2.connect(
#     #     host="host.docker.internal",  
#     #     port=5432,
#     #     database="massmutual_warehouse",
#     #     user="massmutual_user",
#     #     password="massmutual_pass"
#     # )
#     return psycopg2.connect(
#         dbname="massmutual_warehouse",
#         user="massmutual_user",
#         password="massmutual_pass",
#         host="127.0.0.1",  # External host
#         port=5433  # Remapped port!
#     )
# try:
#     conn = get_connection()
#     st.success("Connected to PostgreSQL successfully!")
#     conn.close()
# except Exception as e:
#     st.error(f"Connection failed: {e}")

# # Load Data
# @st.cache_data
# def load_data(query):
#     conn = get_connection()
#     df = pd.read_sql_query(query, conn)
#     conn.close()
#     return df

# # Sidebar Navigation
# st.sidebar.title("Navigation")
# page = st.sidebar.selectbox("Select Section", ["About Pipeline", "Data Preview", "Visualizations"])

# # About Pipeline
# if page == "About Pipeline":
#     st.header("Pipeline Overview")
#     st.write("This dashboard shows the MassMutual Data Pipeline: Ingestion, Validation, Healing, Transformation.")
#     with st.expander("Phase 1: Infrastructure Setup"):
#         st.write("Dockerized Airflow and PostgreSQL stack deployed.")
#     with st.expander("Phase 2: Data Ingestion"):
#         st.write("Loaded 10 Parquet files into PostgreSQL (~1.25M rows).")
#     with st.expander("Phase 3: Enhanced Validation"):
#         st.write("Applied custom rules to check data quality.")
#     with st.expander("Phase 4: Self-Healing"):
#         st.write("Quarantined invalid data (e.g., duplicates, overlapping policies). Cleaned counts: customers 23,953, policies 49,999, claims 125,075, payments 15,010.")
#     with st.expander("Phase 5: Transformation"):
#         st.write("Aggregated data into policy_summary and claims_trends tables, automated via Airflow.")

# # Data Preview
# elif page == "Data Preview":
#     st.header("Data Preview")
#     data_type = st.selectbox("Select Data Type", ["Raw Data", "Cleaned Data", "Quarantine Data", "Transformed Data"])

#     if data_type == "Raw Data":
#         st.subheader("Raw Data Sample (customers table)")
#         df = load_data("SELECT * FROM public.customers LIMIT 100")
#         st.dataframe(df)
#     elif data_type == "Cleaned Data":
#         st.subheader("Cleaned Data Sample (policies table)")
#         df = load_data("SELECT * FROM public.policies LIMIT 100")
#         st.dataframe(df)
#     elif data_type == "Quarantine Data":
#         st.subheader("Quarantine Data Sample")
#         df = load_data("SELECT table_name, COUNT(*) FROM public.quarantine GROUP BY table_name")
#         st.dataframe(df)
#     elif data_type == "Transformed Data":
#         st.subheader("Transformed Data Sample (policy_summary)")
#         df = load_data("SELECT * FROM transformed.policy_summary LIMIT 100")
#         st.dataframe(df)

# # Visualizations
# elif page == "Visualizations":
#     st.header("Data Visualizations")

#     col1, col2 = st.columns(2)

#     with col1:
#         st.subheader("Policy Summary")
#         df = load_data("SELECT * FROM transformed.policy_summary")
#         fig = px.bar(df.head(20), x='customer_id', y='total_policies', color='active_policies', title="Top Customers by Policies")
#         st.plotly_chart(fig, use_container_width=True)
#         with st.expander("Info"):
#             st.write("This bar chart shows total and active policies per customer. Calculated as COUNT(policy_id) and SUM(status = 'active').")

#     with col2:
#         st.subheader("Claims Trends")
#         df = load_data("SELECT * FROM transformed.claims_trends")
#         fig = px.line(df, x='claim_month', y='claim_count', color='claim_type', title="Claims Over Time")
#         st.plotly_chart(fig, use_container_width=True)
#         with st.expander("Info"):
#             st.write("This line chart shows monthly claim counts by type. Calculated as COUNT(*) grouped by DATE_TRUNC('month', claim_date) and claim_type.")

#     st.subheader("Data Flow Overview")
#     cleaned_counts = [23953, 49999, 125075, 15010]
#     quarantined_counts = [26047, 297878, 374925, 984990]
#     df_flow = pd.DataFrame({
#         'Table': ['Customers', 'Policies', 'Claims', 'Payments'],
#         'Cleaned': cleaned_counts,
#         'Quarantined': quarantined_counts
#     })
#     fig_flow = px.bar(df_flow, x='Table', y=['Cleaned', 'Quarantined'], barmode='group', title="Cleaned vs Quarantined Counts")
#     st.plotly_chart(fig_flow, use_container_width=True)
#     with st.expander("Info"):
#         st.write("This bar chart compares raw cleaned data to quarantined invalid data. Cleaned: Valid rows after healing. Quarantined: Invalid rows isolated.")

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from datetime import datetime, timedelta
import requests  # NEW: For Airflow API
import os
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

# Initialize chat session (global or session_state)
if "chat_session" not in st.session_state:
    try:
        model = genai.GenerativeModel('gemini-2.0-flash-lite')  # Specify the model
        st.session_state.chat_session = model.start_chat(history=[])
    except Exception as e:
        st.error(f"Failed to initialize chat session: {e}")
        st.session_state.chat_session = None  # Fallback to avoid crash
    
# Page Configuration
st.set_page_config(
    page_title="MassMutual Executive Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for professional styling
# st.markdown("""
# <style>
#     [data-testid="stAppViewContainer"] { background-color: #F5F7FA; }
#     [data-testid="stHeader"] { background-color: #F5F7FA; }
#     .metric-card {
#         background-color: #FFFFFF;
#         border-radius: 8px;
#         padding: 20px;
#         box-shadow: 0 2px 8px rgba(0,0,0,0.08);
#         text-align: center;
#     }
#     .section-header {
#         color: #1E3A8A;
#         font-weight: 600;
#         font-size: 24px;
#         margin-top: 30px;
#         margin-bottom: 15px;
#     }
#     .status-green { color: #00875A; font-weight: bold; }
#     .status-red { color: #DE350B; font-weight: bold; }
#     .status-yellow { color: #FF991F; font-weight: bold; }
# </style>
# """, unsafe_allow_html=True)

# Database Connection
@st.cache_resource
def get_connection():
    try:
        return psycopg2.connect(
            dbname="massmutual_warehouse",
            user="massmutual_user",
            password="massmutual_pass",
            host="127.0.0.1",
            port=5433
        )
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

# Load Data with Caching
@st.cache_data(ttl=300)
def load_data(query):
    conn = get_connection()
    if conn:
        try:
            df = pd.read_sql_query(query, conn)
            return df
        except Exception as e:
            st.error(f"Query failed: {e}")
            return pd.DataFrame()
    return pd.DataFrame()
# Load data globally for chatbot context
policy_summary = load_data("SELECT * FROM transformed.policy_summary LIMIT 50000")
claims_trends = load_data("SELECT claim_month, claim_count, claim_type FROM transformed.claims_trends LIMIT 1000")
total_claims = claims_trends['claim_count'].sum() if not claims_trends.empty and 'claim_count' in claims_trends.columns else 0
# Sidebar Navigation
st.sidebar.image(r"C:\Users\Asus\materials\1.jpg", width=200)
st.sidebar.title("📊 Navigation")
page = st.sidebar.selectbox(
    "Select Dashboard View",
    ["🏠 Executive Summary", "📈 Business Intelligence", "🔍 Data Quality", "📋 Data Explorer", "⚙️ Pipeline Overview", "🤖 AI Assistant"]
)

# NEW: Add Global Filters
st.sidebar.markdown("---")
st.sidebar.subheader("🔧 Global Filters")
date_range = st.sidebar.date_input(
    "Select Date Range",
    [datetime(2025, 1, 1), datetime(2025, 12, 31)],
    min_value=datetime(2020, 1, 1),
    max_value=datetime(2025, 12, 31)
)
if len(date_range) == 2:
    start_date, end_date = date_range
    st.sidebar.write(f"Selected: {start_date} to {end_date}")
else:
    start_date = end_date = None

# NEW: Refresh Button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()  # Use experimental_rerun for stability
    st.sidebar.markdown(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ==================== PAGE: EXECUTIVE SUMMARY ====================
if page == "🏠 Executive Summary":
    st.title("🏢 MassMutual Executive Dashboard")
    st.markdown("### Real-time Business Metrics & Performance Overview")
    
    # MODIFIED: Limit rows and add error handling
    # MODIFIED: Limit rows and add error handling with case-insensitive check
    policy_summary = load_data("SELECT * FROM transformed.policy_summary LIMIT 50000")
    if not policy_summary.empty:
        st.write("Debug: policy_summary columns:", policy_summary.columns.tolist())  # Debug output
        expected_cols = ['total_policies', 'active_policies', 'total_premium']
        if all(col.lower() in [c.lower() for c in policy_summary.columns] for col in expected_cols):
            total_customers = len(policy_summary)
            total_policies = policy_summary['total_policies'].sum()
            active_policies = policy_summary['active_policies'].sum()
            total_premium = policy_summary['total_premium'].sum()
        else:
            total_customers = total_policies = active_policies = total_premium = 0
            st.warning("Policy summary data incomplete or missing columns. Check schema: \d transformed.policy_summary")
    else:
        total_customers = total_policies = active_policies = total_premium = 0
        st.warning("No data in policy_summary table.")
    
    claims_trends = load_data("SELECT claim_month, claim_count, claim_type FROM transformed.claims_trends LIMIT 1000")
    
    # MODIFIED: Validate columns before calculations
    if not policy_summary.empty and all(col in policy_summary.columns for col in ['total_policies', 'active_policies', 'total_premium']):
        total_customers = len(policy_summary)
        total_policies = policy_summary['total_policies'].sum()
        active_policies = policy_summary['active_policies'].sum()
        total_premium = policy_summary['total_premium'].sum()
    else:
        total_customers = total_policies = active_policies = total_premium = 0
        st.warning("Policy summary data incomplete or missing columns.")
    
    total_claims = claims_trends['claim_count'].sum() if not claims_trends.empty and 'claim_count' in claims_trends.columns else 0
    
    # KPI Cards - Top Row
    st.markdown("#### 📊 Key Performance Indicators")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Customers", f"{total_customers:,}", delta="Active")
    with col2:
        st.metric("Total Policies", f"{total_policies:,}", delta=f"{active_policies:,} Active")
    with col3:
        st.metric("Active Policies", f"{active_policies:,}", delta=f"{(active_policies/total_policies*100):.1f}%" if total_policies > 0 else "0%")
    with col4:
        st.metric("Total Premium", f"${total_premium:,.0f}", delta="Revenue")
    with col5:
        st.metric("Total Claims", f"{total_claims:,}", delta="Processed")
    
    st.markdown("---")
    
    # Business Health Metrics
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 💰 Premium Distribution Analysis")
        if not policy_summary.empty and 'total_premium' in policy_summary.columns:
            fig = px.histogram(
                policy_summary,
                x='total_premium',
                nbins=30,
                title="Customer Premium Distribution",
                labels={'total_premium': 'Total Premium ($)', 'count': 'Number of Customers'},
                color_discrete_sequence=['#0052CC']
            )
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
            
            avg_premium = policy_summary['total_premium'].mean()
            median_premium = policy_summary['total_premium'].median()
            st.info(f"📌 **Avg Premium:** ${avg_premium:,.2f} | **Median Premium:** ${median_premium:,.2f}")
        else:
            st.warning("No data available for premium distribution.")
    
    with col2:
        st.markdown("#### 🎯 Policy Activation Rate")
        if not policy_summary.empty and all(col in policy_summary.columns for col in ['active_policies', 'total_policies']):
            policy_summary['activation_rate'] = (policy_summary['active_policies'] / policy_summary['total_policies'] * 100).round(1)
            fig = px.histogram(
                policy_summary,
                x='activation_rate',
                nbins=20,
                title="Policy Activation Rate Distribution",
                labels={'activation_rate': 'Activation Rate (%)', 'count': 'Number of Customers'},
                color_discrete_sequence=['#00875A']
            )
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
            
            avg_activation = policy_summary['activation_rate'].mean()
            st.success(f"📌 **Average Activation Rate:** {avg_activation:.1f}%")
        else:
            st.warning("No data available for activation rate.")
    
    # Top Performers
    st.markdown("---")
    st.markdown("#### 🏆 Top Performing Segments")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Top 10 Customers by Premium Revenue**")
        if not policy_summary.empty and all(col in policy_summary.columns for col in ['customer_id', 'name', 'total_premium', 'active_policies']):
            top_premium = policy_summary.nlargest(10, 'total_premium')[['customer_id', 'name', 'total_premium', 'active_policies']]
            top_premium['total_premium'] = top_premium['total_premium'].apply(lambda x: f"${x:,.0f}")
            st.dataframe(top_premium, hide_index=True, use_container_width=True)
        else:
            st.warning("No data available for top customers by premium.")
    
    with col2:
        st.markdown("**Top 10 Customers by Policy Count**")
        if not policy_summary.empty and all(col in policy_summary.columns for col in ['customer_id', 'name', 'total_policies', 'active_policies']):
            top_policies = policy_summary.nlargest(10, 'total_policies')[['customer_id', 'name', 'total_policies', 'active_policies']]
            st.dataframe(top_policies, hide_index=True, use_container_width=True)
        else:
            st.warning("No data available for top customers by policies.")
            
# ======================================== PAGE: AI ASSISTANT ========================================
elif page == "🤖 AI Assistant":
    import google.generativeai as genai

    st.title("🤖 AI-Powered Visualization Assistant")
    st.markdown("Ask about any chart, data, or insight — for example: *'Describe the premium distribution'* or *'What do the claims trends show?'*")

    # Initialize Gemini chat session if not already in session_state
    if "chat_session" not in st.session_state:
        model = genai.GenerativeModel("gemini-1.5-flash")
        st.session_state.chat_session = model.start_chat(history=[])

    chat_session = st.session_state.chat_session

    # --- Show previous chat messages ---
    if hasattr(chat_session, "history") and chat_session.history:
        for message in chat_session.history:
            # Handle both dict-based and object-based message formats
            if isinstance(message, dict):
                role = message.get("role", "user")
                parts = message.get("parts", [])
                content = parts[0].get("text", "No content") if parts else "No content"
            else:
                role = getattr(message, "role", "user")
                parts = getattr(message, "parts", [])
                content = getattr(parts[0], "text", "No content") if parts else "No content"

            with st.chat_message(role):
                st.markdown(content)

    # --- Handle user input ---
    if prompt := st.chat_input("What's your question about the dashboard?"):
        # Show the user's message in the chat
        with st.chat_message("user"):
            st.markdown(prompt)

        # Add current dashboard context (if available)
        context = ""
        if not policy_summary.empty:
            context += f"Policy summary has {len(policy_summary)} customers with avg premium ${policy_summary['total_premium'].mean():.2f}. "
        if not claims_trends.empty:
            context += f"Claims trends show {total_claims:,} total claims with avg monthly claims {claims_trends.groupby('claim_month')['claim_count'].sum().mean():.0f}. "

        # Enhanced prompt for better structured answers
        enhanced_prompt = f"""
        You are a helpful assistant for the MassMutual Dashboard.
        User query: {prompt}
        Context: {context or 'No current data available.'}
        Respond with structured markdown: Use ## for section headers (e.g., ## Description), - for bullet points, **bold** for emphasis, and keep responses concise and professional.
        """

        # --- Generate AI response with streaming ---
        response = chat_session.send_message(enhanced_prompt, stream=True)

        with st.chat_message("assistant"):
            full_response = ""
            for chunk in response:
                # Each chunk may be a dict or object
                chunk_text = getattr(chunk, "text", "") or (chunk.get("text") if isinstance(chunk, dict) else "")
                if chunk_text:
                    full_response += chunk_text
                    st.markdown(chunk_text)

            # Clean and format markdown
            formatted_response = (
                full_response.strip()
                .replace("\n", " ")
                .replace("Histogram/Distribution Plot:", "## Histogram/Distribution Plot:")
                .replace("Box Plot:", "## Box Plot:")
                .replace("Insights:", "## Insights:")
                .replace("* ", "- ")
            )
            st.markdown(formatted_response)

        # Save chat history (so it shows next time)
        chat_session.history.append({"role": "user", "parts": [{"text": prompt}]})
        chat_session.history.append({"role": "assistant", "parts": [{"text": formatted_response}]})
          
# ==================== PAGE: BUSINESS INTELLIGENCE ====================
elif page == "📈 Business Intelligence":
    st.title("📈 Business Intelligence & Analytics")
    
    # Load data with date filter
    # MODIFIED: Debug and adjust column names based on schema
    policy_summary = load_data("SELECT * FROM transformed.policy_summary LIMIT 50000")
    if not policy_summary.empty:
        st.write("Debug: policy_summary columns:", policy_summary.columns.tolist())  # NEW: Debug output
        expected_cols = ['total_policies', 'active_policies', 'total_premium']  # Adjust based on \d output
        if all(col.lower() in [c.lower() for c in policy_summary.columns] for col in expected_cols):
            total_customers = len(policy_summary)
            total_policies = policy_summary['total_policies'].sum()
            active_policies = policy_summary['active_policies'].sum()
            total_premium = policy_summary['total_premium'].sum()
        else:
            total_customers = total_policies = active_policies = total_premium = 0
            st.warning("Policy summary data incomplete or missing columns. Check schema: \d transformed.policy_summary")
    else:
        total_customers = total_policies = active_policies = total_premium = 0
        st.warning("No data in policy_summary table.")
    
    claims_query = "SELECT claim_month, claim_count, claim_type FROM transformed.claims_trends"
    if start_date and end_date:
        claims_query += f" WHERE claim_month BETWEEN '{start_date}' AND '{end_date}'"
    claims_trends = load_data(claims_query)
    if not claims_trends.empty:
        st.write("Debug: claims_trends columns:", claims_trends.columns.tolist())  # NEW: Debug output
    
    # Claims Analysis
    st.markdown("### 📋 Claims Trend Analysis")
    col1, col2 = st.columns([2, 1])
    
    with col1:
        if not claims_trends.empty and all(col in claims_trends.columns for col in ['claim_month', 'claim_count', 'claim_type']):
            fig = px.line(
                claims_trends,
                x='claim_month',
                y='claim_count',
                color='claim_type',
                title="Monthly Claims Trend by Type",
                markers=True,
                labels={'claim_month': 'Month', 'claim_count': 'Number of Claims', 'claim_type': 'Claim Type'}
            )
            fig.update_layout(height=450, hovermode='x unified')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No data available for claims trends.")
    
    with col2:
        if not claims_trends.empty and 'claim_type' in claims_trends.columns:
            claims_by_type = claims_trends.groupby('claim_type')['claim_count'].sum().reset_index()
            fig = px.pie(
                claims_by_type,
                values='claim_count',
                names='claim_type',
                title="Claims Distribution by Type",
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Plotly
            )
            fig.update_layout(height=450)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No data available for claims distribution.")
    
    # Claims Statistics
    if not claims_trends.empty and 'claim_count' in claims_trends.columns:
        total_claims = claims_trends['claim_count'].sum()
        avg_monthly_claims = claims_trends.groupby('claim_month')['claim_count'].sum().mean()
        st.info(f"📊 **Total Claims Processed:** {total_claims:,} | **Avg Monthly Claims:** {avg_monthly_claims:.0f}")
    
    st.markdown("---")
    
    # Customer Segmentation
    st.markdown("### 👥 Customer Segmentation Analysis")
    
    if not policy_summary.empty and 'total_policies' in policy_summary.columns:
        def segment_customers(row):
            if row['total_policies'] == 1:
                return '1 Policy'
            elif row['total_policies'] <= 2:
                return '2 Policies'
            elif row['total_policies'] <= 5:
                return '3-5 Policies'
            else:
                return '5+ Policies'
        
        policy_summary['segment'] = policy_summary.apply(segment_customers, axis=1)
        segment_counts = policy_summary['segment'].value_counts().reset_index()
        segment_counts.columns = ['segment', 'count']
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                segment_counts,
                x='segment',
                y='count',
                title="Customer Distribution by Policy Count",
                labels={'segment': 'Customer Segment', 'count': 'Number of Customers'},
                color='count',
                color_continuous_scale='Blues'
            )
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            segment_premium = policy_summary.groupby('segment')['total_premium'].sum().reset_index()
            fig = px.bar(
                segment_premium,
                x='segment',
                y='total_premium',
                title="Total Premium Revenue by Segment",
                labels={'segment': 'Customer Segment', 'total_premium': 'Total Premium ($)'},
                color='total_premium',
                color_continuous_scale='Greens'
            )
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Advanced Metrics
    st.markdown("### 🎯 Advanced Business Metrics")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if not policy_summary.empty:
            avg_premium_per_customer = policy_summary['total_premium'].mean()
            avg_policies_per_customer = policy_summary['total_policies'].mean()
            st.metric("Avg Premium/Customer", f"${avg_premium_per_customer:,.2f}")
            st.metric("Avg Policies/Customer", f"{avg_policies_per_customer:.2f}")
    
    with col2:
        if not policy_summary.empty and not claims_trends.empty:
            total_premium = policy_summary['total_premium'].sum()
            total_claims = claims_trends['claim_count'].sum()
            claims_ratio = (total_claims / total_premium * 100000) if total_premium > 0 else 0
            st.metric("Claims per $100K Premium", f"{claims_ratio:.2f}")
            st.metric("Premium per Policy", f"${(total_premium/policy_summary['total_policies'].sum()):,.2f}")
    
    with col3:
        if not policy_summary.empty:
            policy_summary['premium_per_policy'] = policy_summary['total_premium'] / policy_summary['total_policies']
            high_value_customers = len(policy_summary[policy_summary['premium_per_policy'] > policy_summary['premium_per_policy'].median()])
            st.metric("High-Value Customers", f"{high_value_customers:,}")
            st.metric("% of Total Customers", f"{(high_value_customers/len(policy_summary)*100):.1f}%")

# ==================== PAGE: DATA QUALITY ====================
elif page == "🔍 Data Quality":
    st.title("🔍 Data Quality & Pipeline Health")
    
    # Data Quality KPIs
    cleaned_counts = {
        'Customers': 23953,
        'Policies': 49999,
        'Claims': 125075,
        'Payments': 15010
    }
    quarantined_counts = {
        'Customers': 26047,
        'Policies': 297878,
        'Claims': 374925,
        'Payments': 984990
    }
    
    st.markdown("### 📊 Data Quality Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    for idx, (col, table) in enumerate(zip([col1, col2, col3, col4], cleaned_counts.keys())):
        with col:
            cleaned = cleaned_counts[table]
            quarantined = quarantined_counts[table]
            total = cleaned + quarantined
            quality_pct = (cleaned / total * 100) if total > 0 else 0
            
            st.metric(
                f"{table}",
                f"{quality_pct:.1f}%",
                delta=f"{cleaned:,} valid"
            )
    
    st.markdown("---")
    
    # Detailed Quality Visualization
    col1, col2 = st.columns([2, 1])
    
    with col1:
        df_flow = pd.DataFrame({
            'Table': list(cleaned_counts.keys()),
            'Cleaned': list(cleaned_counts.values()),
            'Quarantined': list(quarantined_counts.values())
        })
        
        # MODIFIED: Create chart for Cleaned vs Quarantined
        # fig = {
        #     "type": "chartjs",
        #     "data": {
        #         "labels": df_flow['Table'].tolist(),
        #         "datasets": [
        #             {
        #                 "label": "Cleaned",
        #                 "data": df_flow['Cleaned'].tolist(),
        #                 "backgroundColor": "#00875A",
        #                 "borderColor": "#006644",
        #                 "borderWidth": 1
        #             },
        #             {
        #                 "label": "Quarantined",
        #                 "data": df_flow['Quarantined'].tolist(),
        #                 "backgroundColor": "#DE350B",
        #                 "borderColor": "#A62808",
        #                 "borderWidth": 1
        #             }
        #         ]
        #     },
        #     "options": {
        #         "type": "bar",
        #         "data": {
        #             "labels": df_flow['Table'].tolist(),
        #             "datasets": [
        #                 {
        #                     "label": "Cleaned",
        #                     "data": df_flow['Cleaned'].tolist(),
        #                     "backgroundColor": "#00875A",
        #                     "borderColor": "#006644",
        #                     "borderWidth": 1
        #                 },
        #                 {
        #                     "label": "Quarantined",
        #                     "data": df_flow['Quarantined'].tolist(),
        #                     "backgroundColor": "#DE350B",
        #                     "borderColor": "#A62808",
        #                     "borderWidth": 1
        #                 }
        #             ]
        #         },
        #         "options": {
        #             "plugins": {
        #                 "title": {
        #                     "display": True,
        #                     "text": "Data Quality: Cleaned vs Quarantined Records"
        #                 },
        #                 "legend": {
        #                     "position": "top"
        #                 }
        #             },
        #             "scales": {
        #                 "x": {
        #                     "title": {
        #                         "display": True,
        #                         "text": "Table"
        #                     }
        #                 },
        #                 "y": {
        #                     "title": {
        #                         "display": True,
        #                         "text": "Record Count"
        #                     },
        #                     "beginAtZero": True
        #                 }
        #             }
        #         }
        #     }
        # }
        # st.plotly_chart(fig, use_container_width=True)
        # FIXED: Use Plotly instead of Chart.js
        fig = px.bar(
            df_flow,
            x='Table',
            y=['Cleaned', 'Quarantined'],
            barmode='group',
            title="Data Quality: Cleaned vs Quarantined Records",
            labels={'value': 'Record Count', 'variable': 'Data Status'},
            color_discrete_map={'Cleaned': '#00875A', 'Quarantined': '#DE350B'}
        )
        fig.update_layout(height=450)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("#### 📈 Quality Metrics")
        for table in cleaned_counts.keys():
            cleaned = cleaned_counts[table]
            quarantined = quarantined_counts[table]
            total = cleaned + quarantined
            quality_pct = (cleaned / total * 100) if total > 0 else 0
            
            st.markdown(f"**{table}**")
            st.progress(quality_pct / 100)
            st.caption(f"✅ {cleaned:,} valid | ❌ {quarantined:,} quarantined")
            st.markdown("")
    
    st.markdown("---")
    
    # Quarantine Data Analysis
    st.markdown("### 🚨 Quarantine Data Analysis")
    
    # MODIFIED: Add fallback for missing table_name/reason
    try:
        quarantine_data = load_data("SELECT table_name, error_message, COUNT(*) as count FROM public.quarantine GROUP BY table_name, error_message ORDER BY count DESC LIMIT 20")
    except:
        quarantine_data = pd.DataFrame({
            'table_name': ['customers', 'policies', 'claims', 'payments'],
            'error_message': ['unknown', 'unknown', 'unknown', 'unknown'],
            'count': [26047, 297878, 374925, 984990]
        })
    
    if not quarantine_data.empty:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            fig = px.bar(
                quarantine_data.head(10),
                x='count',
                y='error_message',
                color='table_name',
                orientation='h',
                title="Top 10 Quarantine Error Messages",
                labels={'count': 'Number of Records', 'error_message': 'Error Message'}
            )
            fig.update_layout(height=450, yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("#### 📋 Quarantine Summary")
            try:
                quarantine_by_table = load_data("SELECT table_name, COUNT(*) as count FROM public.quarantine GROUP BY table_name ORDER BY count DESC")
            except:
                quarantine_data = pd.DataFrame({
                    'table_name': ['customers', 'policies', 'claims', 'payments'],
                    'error_message': ['unknown', 'unknown', 'unknown', 'unknown'],
                    'count': [26047, 297878, 374925, 984990]
                })
            if not quarantine_by_table.empty:
                st.dataframe(quarantine_by_table, hide_index=True, use_container_width=True)
    
    # Pipeline Health Indicators
    st.markdown("---")
    st.markdown("### ⚡ Pipeline Health Indicators")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("#### 🟢 Ingestion")
        st.success("**Status:** Healthy")
        st.caption("10 Parquet files loaded")
    
    with col2:
        st.markdown("#### 🟢 Validation")
        st.success("**Status:** Active")
        st.caption("Custom rules applied")
    
    with col3:
        st.markdown("#### 🟢 Transformation")
        st.success("**Status:** Complete")
        st.caption("2 aggregation tables")
    
    with col4:
        st.markdown("#### 🟢 Automation")
        st.success("**Status:** Scheduled")
        st.caption("Airflow DAG running")

# ==================== PAGE: DATA EXPLORER ====================
elif page == "📋 Data Explorer":
    st.title("📋 Data Explorer")
    st.markdown("### Interactive Data Tables & Detailed Views")
    
    data_category = st.selectbox(
        "Select Data Category",
        ["Transformed Data", "Raw Data", "Quarantine Data"]
    )
    
    if data_category == "Transformed Data":
        table_choice = st.radio("Select Table", ["Policy Summary", "Claims Trends"])
        
        if table_choice == "Policy Summary":
            st.markdown("#### 📊 Policy Summary Table")
            df = load_data("SELECT * FROM transformed.policy_summary ORDER BY total_premium DESC LIMIT 50000")
            
            if not df.empty:
                col1, col2 = st.columns(2)
                with col1:
                    min_policies = st.slider("Minimum Total Policies", 0, int(df['total_policies'].max()), 0)
                with col2:
                    min_premium = st.number_input("Minimum Total Premium ($)", 0, int(df['total_premium'].max()), 0)
                
                filtered_df = df[(df['total_policies'] >= min_policies) & (df['total_premium'] >= min_premium)]
                
                st.info(f"📊 Showing {len(filtered_df):,} of {len(df):,} records")
                st.dataframe(filtered_df, use_container_width=True, height=500)
                
                csv = filtered_df.to_csv(index=False)
                st.download_button("📥 Download CSV", csv, "policy_summary.csv", "text/csv")
        
        else:  # Claims Trends
            st.markdown("#### 📈 Claims Trends Table")
            claims_query = "SELECT * FROM transformed.claims_trends"
            if start_date and end_date:
                claims_query += f" WHERE claim_month BETWEEN '{start_date}' AND '{end_date}'"
            claims_query += " ORDER BY claim_month DESC, claim_type"  # Corrected order: WHERE before ORDER BY
            df = load_data(claims_query)
            
            if not df.empty:
                st.write("Debug: claims_trends columns:", df.columns.tolist())  # NEW: Debug output
                claim_types = ['All'] + sorted(df['claim_type'].unique().tolist())
                selected_type = st.selectbox("Filter by Claim Type", claim_types)
                
                if selected_type != 'All':
                    filtered_df = df[df['claim_type'] == selected_type]
                else:
                    filtered_df = df
                
                st.info(f"📊 Showing {len(filtered_df):,} records")
                st.dataframe(filtered_df, use_container_width=True, height=500)
                
                csv = filtered_df.to_csv(index=False)
                st.download_button("📥 Download CSV", csv, "claims_trends.csv", "text/csv")
            else:
                st.warning("No data available in claims_trends table. Check schema: \d transformed.claims_trends")
    
    elif data_category == "Raw Data":
        # MODIFIED: Include additional tables from schema
        table_choice = st.selectbox("Select Table", ["Customers", "Policies", "Claims", "Payments", "Agents", "Branches", "Coverage Levels", "Currency Rates", "Policy Types", "Payment Methods"])
        
        st.markdown(f"#### 📄 {table_choice} Table (Sample)")
        
        table_map = {
            "Customers": "public.customers",
            "Policies": "public.policies",
            "Claims": "public.claims",
            "Payments": "public.payments",
            "Agents": "public.agents",
            "Branches": "public.branches",
            "Coverage Levels": "public.coverage_levels",
            "Currency Rates": "public.currency_rates",
            "Policy Types": "public.policy_types",
            "Payment Methods": "public.payment_methods"
        }
        
        limit = st.slider("Number of rows to display", 10, 500, 100)
        df = load_data(f"SELECT * FROM {table_map[table_choice]} LIMIT {limit}")
        
        if not df.empty:
            st.dataframe(df, use_container_width=True, height=500)
            
            csv = df.to_csv(index=False)
            st.download_button("📥 Download Sample", csv, f"{table_choice.lower().replace(' ', '_')}_sample.csv", "text/csv")
        else:
            st.warning(f"No data available in {table_choice} table. Check schema: \d {table_map[table_choice]}")
    
    else:  # Quarantine Data
        st.markdown("#### 🚨 Quarantine Data Analysis")
        
        try:
            df = load_data("SELECT table_name, error_message, COUNT(*) as count FROM public.quarantine GROUP BY table_name, error_message ORDER BY table_name, count DESC")
        except Exception as e:
            st.error(f"Query failed for quarantine data: {e}")
            df = pd.DataFrame({
                'table_name': ['customers', 'policies', 'claims', 'payments'],
                'error_message': ['unknown', 'unknown', 'unknown', 'unknown'],
                'count': [26047, 297878, 374925, 984990]
            })
        
        if not df.empty:
            selected_table = st.selectbox("Filter by Source Table", ['All'] + sorted(df['table_name'].unique().tolist()))
            
            if selected_table != 'All':
                filtered_df = df[df['table_name'] == selected_table]
            else:
                filtered_df = df
            
            st.dataframe(filtered_df, use_container_width=True, height=400)
            
            st.markdown("#### 📋 Sample Quarantined Records")
            sample_limit = st.slider("Sample size", 10, 100, 50)
            
            try:
                if selected_table != 'All':
                    sample_df = load_data(f"SELECT * FROM public.quarantine WHERE table_name = '{selected_table}' LIMIT {sample_limit}")
                else:
                    sample_df = load_data(f"SELECT * FROM public.quarantine LIMIT {sample_limit}")
            except Exception as e:
                st.error(f"Query failed for sample quarantine data: {e}")
                sample_df = pd.DataFrame()
            
            if not sample_df.empty:
                st.dataframe(sample_df, use_container_width=True, height=300)
            else:
                st.warning("No sample quarantine data available.")

# ==================== PAGE: PIPELINE OVERVIEW ====================
elif page == "⚙️ Pipeline Overview":
    st.title("⚙️ Data Pipeline Architecture")
    st.markdown("### End-to-End Pipeline Workflow & Status")
    
    # NEW: Airflow DAG Status
    st.markdown("### 📡 Airflow Pipeline Status")
    try:
        response = requests.get(
                "http://localhost:8080/api/v1/dags/transform_massmutual_manual/dagRuns",
                auth=('admin', 'admin'),  # Confirmed working credentials
                timeout=5  # Added to avoid hanging
            )
        if response.status_code == 200:
            dag_runs = response.json()['dag_runs']
            latest_run = dag_runs[0] if dag_runs else None
            if latest_run:
                status = latest_run['state']
                status_color = "status-green" if status == "success" else "status-red" if status == "failed" else "status-yellow"
                st.markdown(f"**Latest DAG Run:** <span class='{status_color}'>{status}</span> at {latest_run['execution_date']}", unsafe_allow_html=True)
            else:
                st.warning("No DAG runs found.")
        else:
            st.error(f"Airflow API failed: {response.status_code}")
    except Exception as e:
        st.info(f"Airflow API error: {e}. Check http://localhost:8080")
    st.markdown("---")
    
    # Pipeline Phases
    phases = [
        {
            "phase": "Phase 1: Infrastructure",
            "status": "✅ Complete",
            "description": "Dockerized Airflow + PostgreSQL stack deployed",
            "details": ["Docker Compose setup", "Airflow web UI accessible", "PostgreSQL database initialized"]
        },
        {
            "phase": "Phase 2: Data Ingestion",
            "status": "✅ Complete",
            "description": "Loaded 10 Parquet files into PostgreSQL (~1.25M rows)",
            "details": ["Parquet file parsing", "Bulk insert operations", "Data integrity checks"]
        },
        {
            "phase": "Phase 3: Enhanced Validation",
            "status": "✅ Complete",
            "description": "Applied custom validation rules to ensure data quality",
            "details": ["Duplicate detection", "Date range validation", "Foreign key checks", "Business rule validation"]
        },
        {
            "phase": "Phase 4: Self-Healing",
            "status": "✅ Complete",
            "description": "Quarantined invalid data and cleaned datasets",
            "details": [
                "Customers: 23,953 cleaned",
                "Policies: 49,999 cleaned",
                "Claims: 125,075 cleaned",
                "Payments: 15,010 cleaned"
            ]
        },
        {
            "phase": "Phase 5: Transformation",
            "status": "✅ Complete",
            "description": "Aggregated data into analytical tables",
            "details": [
                "policy_summary: 23,953 rows",
                "claims_trends: 483 rows",
                "Automated via Airflow DAG"
            ]
        },
        {
            "phase": "Phase 7: Production Dashboard",
            "status": "✅ Active",
            "description": "Streamlit dashboard for data visualization",
            "details": ["Executive summary", "Business intelligence", "Data quality monitoring"]
        },
        {
            "phase": "Phase 8: Production Ready",
            "status": "⏳ Pending",
            "description": "Optimization and deployment preparation",
            "details": ["Performance indexing", "Error handling", "Scheduling optimization", "Deployment strategy"]
        }
    ]
    
    # Display phases
    for phase_info in phases:
        with st.expander(f"{phase_info['status']} {phase_info['phase']}", expanded=False):
            st.markdown(f"**Description:** {phase_info['description']}")
            st.markdown("**Key Features:**")
            for detail in phase_info['details']:
                st.markdown(f"- {detail}")
    
    st.markdown("---")
    
    # Pipeline Metrics
    st.markdown("### 📊 Pipeline Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Phases Completed", "7/8", delta="87.5%")
    with col2:
        st.metric("Data Tables", "11", delta="2 transformed")  # MODIFIED: Updated to 11 tables
    with col3:
        st.metric("Records Processed", "1.25M+", delta="198K cleaned")
    with col4:
        st.metric("Automation Status", "Active", delta="Airflow DAG")
    
    st.markdown("---")
    
    # Architecture Diagram (Text-based)
    st.markdown("### 🏗️ System Architecture")
    
    st.code("""
    ┌─────────────────────────────────────────────────────────────────┐
    │                        DATA SOURCES                              │
    │                   (10 Parquet Files)                             │
    └─────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │                    INGESTION LAYER                               │
    │              (Airflow DAG: Bulk Insert)                          │
    └─────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │                   VALIDATION LAYER                               │
    │         (Custom Rules: Duplicates, Dates, FK)                    │
    └─────────────────────────────────────────────────────────────────┘
                                   │
                        ┌──────────┴──────────┐
                        ▼                     ▼
    ┌──────────────────────────┐  ┌──────────────────────────┐
    │     CLEANED DATA         │  │   QUARANTINE TABLE       │
    │  (Valid Records)         │  │  (Invalid Records)       │
    └──────────────────────────┘  └──────────────────────────┘
                        │
                        ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │                TRANSFORMATION LAYER                              │
    │        (SQL Aggregations: policy_summary, claims_trends)         │
    └─────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │                  PRESENTATION LAYER                              │
    │              (Streamlit Dashboard)                               │
    └─────────────────────────────────────────────────────────────────┘
    """, language="text")
    
    st.markdown("---")
    
    # Database Schema
    st.markdown("### 🗄️ Database Schema")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Raw & Cleaned Tables")
        st.markdown("""
        - `public.customers` - Customer master data
        - `public.policies` - Policy information
        - `public.claims` - Claims records
        - `public.payments` - Payment transactions
        - `public.agents` - Agent information
        - `public.branches` - Branch details
        - `public.coverage_levels` - Coverage tiers
        - `public.currency_rates` - Currency exchange rates
        - `public.policy_types` - Policy categories
        - `public.payment_methods` - Payment options
        - `public.quarantine` - Invalid records
        """)
    
    with col2:
        st.markdown("#### Transformed Tables")
        st.markdown("""
        - `transformed.policy_summary` - Customer policy aggregations
        - `transformed.claims_trends` - Monthly claim trends by type
        """)
    
    st.markdown("---")
    
    # Next Steps
    st.markdown("### 🎯 Next Steps & Recommendations")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🚀 Immediate Actions")
        st.markdown("""
        1. **Performance Optimization**
           - Add database indexes on key columns
           - Optimize query performance
           - Implement connection pooling
        """)
    
    with col2:
        st.markdown("#### 📈 Future Enhancements")
        st.markdown("""
        1. **Advanced Analytics**
           - Customer Lifetime Value (CLV) modeling
           - Churn prediction models
           - Geographic analysis with maps
           - Profitability analysis by policy type
        2. **Production Deployment**
           - Cloud deployment (AWS/Azure/GCP)
           - CI/CD pipeline setup
           - Monitoring and logging
           - Backup and disaster recovery
        """)