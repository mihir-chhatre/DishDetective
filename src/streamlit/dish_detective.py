import streamlit as st
import folium
from streamlit_folium import st_folium
import matplotlib.pyplot as plt

from google.cloud import bigquery



####################################################################################

######  Geospatial analysis   ######

client = bigquery.Client()

sql = """
    SELECT *
    FROM `bigdata-ripp.ripp_analytics.dohmh_most_recent_letter_grade`
"""

df = client.query(sql).to_dataframe()

# Remove rows where either latitude or longitude is NaN
df = df.dropna(subset=['latitude', 'longitude'])

st.header("Geospatial Analysis to help diners pick restaurants")

# Dropdowns for user input in the main area
borough = st.selectbox('Borough:', df['boro'].unique().tolist())
cuisine = st.selectbox('Cuisine:', df['cuisine_description'].unique().tolist())

# Filter data based on user input
filtered_data = df[
    (df['boro'] == borough) & 
    (df['cuisine_description'] == cuisine)
]

# Initialize map
def init_map():
    return folium.Map(location=[40.7128, -74.0060], zoom_start=12)

# Add markers to the map
m = init_map()
for index, row in filtered_data.iterrows():
    if row['grade'] == 'A':
        marker_color = 'green'
    elif row['grade'] == 'B':
        marker_color = 'orange'
    elif row['grade'] == 'C':
        marker_color = 'red'
    else:
        marker_color = 'gray'  # For missing or other grades

    tooltip_text = f"{row['dba']}<br>Phone: {row['phone']}<br>Grade: {row['grade']}"
    folium.CircleMarker(
        location=[row['latitude'], row['longitude']],
        radius=5,
        color=marker_color,
        fill=True,
        fill_color=marker_color,
        tooltip=tooltip_text
    ).add_to(m)

# Display the map
st_folium(m, width=700, height=500)


####################################################################################


######  Cuisine violation analysis  ######


sql = """
    SELECT *
    FROM `bigdata-ripp.ripp_analytics.dohmh_cuisine_violation_analysis`
"""

cuisine_violation_analysis_pd = client.query(sql).to_dataframe()

st.write("")
st.header("Cuisine based Violation Analysis")

# Sidebar for user inputs in second section
cuisine_violation = st.selectbox('Select Cuisine for Violation Analysis:', ['All'] + cuisine_violation_analysis_pd['cuisine_description'].unique().tolist())

# Display top violations for selected cuisine
def show_top_violations(selected_cuisine):
    if selected_cuisine == 'All':
        display_df = cuisine_violation_analysis_pd[cuisine_violation_analysis_pd["rank"] <= 5]
    else:
        display_df = cuisine_violation_analysis_pd[
            (cuisine_violation_analysis_pd["cuisine_description"] == selected_cuisine) &
            (cuisine_violation_analysis_pd["rank"] <= 5)
        ]
    return display_df

top_violations_df = show_top_violations(cuisine_violation)
st.write('Top Violations for Selected Cuisine:', top_violations_df)




####################################################################################




######  Violation Code per Year Analysis  ######

sql = """
    SELECT *
    FROM `bigdata-ripp.ripp_analytics.dohmh_violations_per_year`
"""

violations_per_year_pd = client.query(sql).to_dataframe()

st.write("")
st.header("Violation codes across the years")

# Dropdown for selecting violation code
selected_violation_code = st.selectbox('Select Violation Code:', violations_per_year_pd['violation_code'].unique())

# Filter data based on selected violation code
filtered_grouped_data = violations_per_year_pd[violations_per_year_pd['violation_code'] == selected_violation_code]

# Sort the data by year
filtered_grouped_data = filtered_grouped_data.sort_values('inspection_year')

# Create a bar plot
fig, ax = plt.subplots()
ax.bar(filtered_grouped_data['inspection_year'], filtered_grouped_data['count'], color='skyblue')
ax.set_xlabel('Year')
ax.set_ylabel('Count')
ax.set_title(f'Violation Counts Over Years for Code {selected_violation_code}')

# Display the plot
st.pyplot(fig)


####################################################################################



######  Seasonal Trend of Specific Violation  ######

sql = """
    SELECT *
    FROM `bigdata-ripp.ripp_analytics.dohmh_season_trend_violations`
"""

st.write("")
st.header("Seasonal trends of specific Violations")

# User input for violation code
violation_code_input = st.text_input("Please enter the violation code: ")

if violation_code_input:
    season_trend_specific_violation = client.query(sql).to_dataframe()

    season_trend_specific_violation = season_trend_specific_violation[season_trend_specific_violation["violation_code"] == violation_code_input]

    # Sort and plot the data
    season_trend_specific_violation = season_trend_specific_violation.sort_values('season')
    fig, ax = plt.subplots()
    ax.plot(season_trend_specific_violation['season'], season_trend_specific_violation['count'], marker='o')
    ax.set_xlabel('Season')
    ax.set_ylabel('Count')
    ax.set_title(f'Seasonal Trend for Violation Code {violation_code_input}')

    # Display the plot
    st.pyplot(fig)





####################################################################################


######  Violations across boroughs  ######

st.write("")
st.header("Top five violations across Boroughs")

sql = """
    SELECT *
    FROM `bigdata-ripp.ripp_analytics.dohmh_top_violations_per_boro`
"""

top_violations_per_boro_pd = client.query(sql).to_dataframe()

# Dropdown for borough selection
selected_borough = st.selectbox("Select a Borough", options=top_violations_per_boro_pd["boro"].unique())

# Filtering data based on selected borough
filtered_data = top_violations_per_boro_pd[top_violations_per_boro_pd["boro"] == selected_borough]

# Plotting
fig, ax = plt.subplots()
fig.patch.set_facecolor('black')  # Set the background of the figure to black
ax.set_facecolor('black')         # Set the background of the axes to black

# Change the text color to white for better visibility on black background
# Place percentages outside the slices
ax.pie(filtered_data["count"], labels=filtered_data["violation_code"], labeldistance=0.5, 
       autopct=lambda p: f'{p:.1f}%', pctdistance=1.2, textprops={'color': "white"})
ax.axis('equal')  # Equal aspect ratio ensures the pie chart is circular.

# Show plot in Streamlit
st.pyplot(fig)



####################################################################################



######  Restaurant chain analysis  ######

st.write("")
st.header("Restaurant chain analysis")

sql = """
    SELECT *
    FROM `bigdata-ripp.ripp_analytics.dohmh_restaurant_chain_analysis`
"""

result = client.query(sql).to_dataframe()

# Extract unique DBA values for the dropdown
unique_dbas = result['dba'].drop_duplicates()

# Streamlit dropdown for DBA selection
selected_dba = st.selectbox(
    'Select DBA:',
    unique_dbas.tolist()
)

# Display data for the selected DBA
if selected_dba:
    # Filter the DataFrame for the selected DBA
    filtered_data = result[result['dba'] == selected_dba]

    # Display DBA and Total Locations
    if not filtered_data.empty:
        dba_info = filtered_data.iloc[0][['dba', 'TotalLocations']]
        st.write(f"Total Locations: {dba_info['TotalLocations']}")

        # Drop the DBA and TotalLocations columns for display
        filtered_data = filtered_data.drop(columns=['dba', 'TotalLocations'])

    st.dataframe(filtered_data)



####################################################################################


######  Violation Code Description  ######

sql = """
    SELECT *
    FROM `bigdata-ripp.ripp_analytics.dohmh_most_recent_letter_grade`
"""

most_recent_letter_grade = client.query(sql).to_dataframe()
most_recent_letter_grade = most_recent_letter_grade[most_recent_letter_grade['violation_code'].notna()]

violation_codes_df = most_recent_letter_grade[['violation_code', 'violation_description']].drop_duplicates()

st.sidebar.header("Want to know more about a violation code?")

# Dropdown for selecting violation code
selected_violation_code = st.sidebar.selectbox('Select a Violation Code:', violation_codes_df['violation_code'].unique())

# Get the description of the selected violation code
selected_violation_description = violation_codes_df[violation_codes_df['violation_code'] == selected_violation_code]['violation_description'].iloc[0]

# Display the description in the sidebar
st.sidebar.write("Description: ", selected_violation_description)