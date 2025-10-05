import requests
import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector import Error

API_KEY = "4e977655-299d-4aa4-b2db-92c1e7ae7cea"
url = "https://api.harvardartmuseums.org/object"

# -------------------------------
# MySQL connection function
# -------------------------------
def get_connection():
    return mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="@Wrzsrav@7601",   # your MySQL password
        database="Harvard_Artifacts_Collection"
    )

# -------------------------------
# Dropdown for classification
# -------------------------------
st.title("🏛️ Harvard’s Artifacts Collection")

classification = st.selectbox(
    "Select a classification:",
    ["Coins", "Paintings", "Sculpture", "Jewellery", "Drawings","Accessories "]
)

# -------------------------------
# Collect Data Button
# -------------------------------
if st.button("Collect Data"):
    data_records = []
    for page in range(1, 26):   # 25 pages × 100 = 2500 records
        params = {
            "apikey": API_KEY,
            "size": 100,
            "page": page,
            "classification": classification
        }
        response = requests.get(url, params=params)
        data = response.json()
        page_records = data.get("records", [])
        data_records.extend(page_records)

    st.success(f"✅ Total records collected: {len(data_records)} for {classification}")
    st.session_state["records"] = data_records  # store raw records

# -------------------------------
# Show Data Button
# -------------------------------
if st.button("Show Data"):
    if "records" in st.session_state:
        data_records = st.session_state["records"]

        artifact_metadata = []
        artifact_media = []
        artifact_colors = []

        for i in data_records:
            artifact_metadata.append(dict(
                id=i.get('id'),
                title=i.get('title'),
                culture=i.get('culture'),
                period=i.get('period'),
                century=i.get('century'),
                medium=i.get('medium'),
                dimensions=i.get('dimensions'),
                description=i.get('description'),
                department=i.get('department'),
                classification=i.get('classification'),
                accessionyear=i.get('accessionyear'),
                accessionmethod=i.get('accessionmethod')
            ))

            artifact_media.append(dict(
                objectid=i.get('objectid'),
                imagecount=i.get('imagecount'),
                mediacount=i.get('mediacount'),
                colorcount=i.get('colorcount'),
                rank=i.get('rank'),
                datebegin=i.get('datebegin'),
                dateend=i.get('dateend')
            ))

            color_details = i.get('colors')
            if color_details:
                for j in color_details:
                    artifact_colors.append(dict(
                        objectid=i.get('objectid'),
                        hue=j.get('hue'),
                        color=j.get('color'),
                        spectrum=j.get('spectrum'),
                        percent=j.get('percent'),
                        css3=j.get('css3')
                    ))

        # Convert to DataFrames for preview
        df_meta = pd.DataFrame(artifact_metadata)
        df_media = pd.DataFrame(artifact_media)
        df_colors = pd.DataFrame(artifact_colors)

        # Show preview
        st.subheader("🗄️ Artifact Metadata")
        st.dataframe(df_meta.head(20))
        st.subheader("🖼️ Artifact Media")
        st.dataframe(df_media.head(20))
        st.subheader("🎨 Artifact Colors")
        st.dataframe(df_colors.head(20))

        # Save DataFrames in session
        st.session_state["artifact_metadata"] = artifact_metadata
        st.session_state["artifact_media"] = artifact_media
        st.session_state["artifact_colors"] = artifact_colors

    else:
        st.warning("⚠️ No data available. Please collect data first!")

# -------------------------------
# Insert into SQL Button
# -------------------------------
if st.button("Insert into SQL"):
    if "artifact_metadata" in st.session_state:
        artifact_metadata = st.session_state["artifact_metadata"]
        artifact_media = st.session_state["artifact_media"]
        artifact_colors = st.session_state["artifact_colors"]

        try:
            conn = get_connection()
            cursor = conn.cursor()

            # ------------------ Create Tables ------------------
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS artifact_metadata (
                   id INT PRIMARY KEY, 
                   title VARCHAR(255),
                   culture VARCHAR(255),
                   period VARCHAR(255),
                   century VARCHAR(255),
                   medium VARCHAR(255),
                   dimensions VARCHAR(255),
                   description TEXT,
                   department VARCHAR(255), 
                   classification VARCHAR(255),
                   accessionyear INT,
                   accessionmethod VARCHAR(255)
               )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS artifact_media (
                 objectid   INT ,
                 imagecount INT,
                 mediacount INT,
                 colorcount INT,
                 rank_num   INT,
                 datebegin  INT,
                 dateend    INT,
                 FOREIGN KEY (objectid) REFERENCES artifact_metadata(id)
             )
            """)

            cursor.execute("""
               CREATE TABLE IF NOT EXISTS artifact_colors (
                 objectid INT ,
                 color TEXT,
                 spectrum VARCHAR(255),
                 hue VARCHAR(255),
                 percent REAL,
                 css3  VARCHAR(255),
                 FOREIGN KEY (objectid) REFERENCES artifact_media(objectid)
             )
            """)

            # ------------------ Insert Metadata ------------------
            for item in artifact_metadata:
               cursor.execute("""
                   INSERT INTO artifact_metadata (
                       id, title, culture, period, century, medium, dimensions,
                       description, department, classification,
                       accessionyear, accessionmethod
                   ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
               """, (
                   item['id'], item['title'], item['culture'], item['period'], item['century'], item['medium'],
                   item['dimensions'], item['description'], item['department'], item['classification'],
                   item['accessionyear'], item['accessionmethod']
                ))

            # ------------------ Insert Media ------------------
            for item in artifact_media:
               cursor.execute("""
                   INSERT INTO artifact_media (
                       objectid, imagecount, mediacount, colorcount, rank_num, datebegin, dateend
                   ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                   item['objectid'], item['imagecount'], item['mediacount'], item['colorcount'], item['rank'], item['datebegin'],
                   item['dateend']
               ))

            # ------------------ Insert Colors ------------------
            for item in artifact_colors:
               cursor.execute("""
                   INSERT INTO artifact_colors (
                       objectid, color, spectrum, hue, percent, css3
                   ) VALUES (%s, %s, %s, %s, %s, %s)
               """, (
                   item['objectid'], item['color'], item['spectrum'], item['hue'], item['percent'], item['css3']
               ))

            # Commit all changes
            conn.commit()
            st.success("✅ Data inserted into MySQL successfully!")

        except Error as e:
            st.error(f"❌ Error while inserting into MySQL: {e}")

        finally:
            if conn.is_connected():
                conn.close()
                st.info("🔒 MySQL connection closed.")
    else:
        st.error("⚠️ Please show data before inserting into SQL.")


# -------------------------------
# Query & Visualization Section
# -------------------------------
st.subheader("🔎 Query & Visualization Section")

queries = {
    "Artifacts from the 11th century (Byzantine culture)": """
        SELECT * FROM artifact_metadata 
        WHERE century = '11th century' AND culture = 'Byzantine';
    """,
    "Unique cultures represented": """
        SELECT DISTINCT culture FROM artifact_metadata;
    """,
    "Artifacts from Archaic Period": """
        SELECT * FROM artifact_metadata WHERE period = 'Archaic';
    """,
    "Artifacts ordered by accession year": """
        SELECT title, accessionyear 
        FROM artifact_metadata 
        ORDER BY accessionyear DESC;
    """,
    "Artifacts count per department": """
        SELECT department, COUNT(*) as total 
        FROM artifact_metadata 
        GROUP BY department;
    """,
    "Artifacts with more than 1 image": """
        SELECT * FROM artifact_media WHERE imagecount > 1;
    """,
    "Average rank of artifacts": """
        SELECT AVG(rank) as avg_rank FROM artifact_media;
    """,
    "Artifacts having a higher colorcount than mediacount": """
        select objectid,colorcount, mediacount from artifact_media where colorcount> mediacount
        order by objectid;
    """,
    "All artifacts created between 1500 and 1600":"""
        select objectid,datebegin,dateend from artifact_media
        where datebegin>= 1500 and dateend<=1600
        order by objectid;
    """,
    "Count of artifacts having no media files":"""
        select count(*) as no_media_files from artifact_media where 
        mediacount='0';
    """,
    "All the distinct hues used in the artifact_colors":"""
        select distinct(hue) from artifact_colors;
    """,
    "Top 5 most used colors by frequency":"""
        select  hue,count(*) as frequency  from artifact_colors 
        group by hue
        order by frequency desc
        limit 5;
    """,
    "Average coverage percentage for each hue":"""
        select hue,avg(percent) as avg_percent from artifact_colors 
        group by hue
        order by avg_percent desc;
    """,
    "Total number of color entries in the dataset":"""
        select count(color) as total_color_entry  from artifact_colors;
    """,
    "List artifact titles and hues for all artifacts belonging to the Byzantine culture":"""
       select a.title,b.hue,a.culture from artifact_metadata a
       inner join  artifact_colors b on a.id=b.objectid
       where a.culture='Byzantine';
    """,
    "List each artifact title with its associated hues":"""
       select a.title ,GROUP_CONCAT(b.hue) AS hues from artifact_metadata a
       inner join  artifact_colors b
       on a.id=b.objectid
       group by  a.title;
    """,
    "Artifact titles, cultures, and media ranks where the period is not null":"""
       select a.title,a.culture, b.rank_num from artifact_metadata a
       inner join artifact_media b
       on a.id=b.objectid 
       where a.period is not null;
    """,
    "Artifact titles ranked in the top 10 that include the color hue is Grey":"""
       select a.title , b.hue from artifact_metadata a
       inner join artifact_colors b on a.id=b.objectid
       inner join artifact_media c on b.objectid=c.objectid 
       where b.hue='grey'
       order by c.rank_num desc
       limit 10;
    """,
    "Artifacts exists per classification, and what is the average media count for each":"""
       SELECT 
       a.classification, 
       COUNT(*) AS artifact_count, 
       AVG(b.mediacount) AS avg_media_count
       FROM artifact_metadata a
       inner join artifact_media b on a.id=b.objectid 
       GROUP BY a.classification
       order by artifact_count desc;
    """,
    "Top 10 oldest artifacts (by datebegin)": """
        SELECT title, culture, datebegin 
        FROM artifact_media 
        JOIN artifact_metadata ON artifact_media.objectid = artifact_metadata.id
        WHERE datebegin IS NOT NULL
        ORDER BY datebegin ASC
        LIMIT 10;
    """,

    "Average number of images per classification": """
        SELECT m.classification, AVG(a.imagecount) AS avg_images
        FROM artifact_media a
        JOIN artifact_metadata m ON a.objectid = m.id
        GROUP BY m.classification
        ORDER BY avg_images DESC;
    """,

    "Most common colors used across artifacts": """
        SELECT color, COUNT(*) as frequency
        FROM artifact_colors
        GROUP BY color
        ORDER BY frequency DESC
        LIMIT 10;
    """,

    "Departments with the most unique cultures": """
        SELECT department, COUNT(DISTINCT culture) AS unique_cultures
        FROM artifact_metadata
        GROUP BY department
        ORDER BY unique_cultures DESC;
    """,

    "Artifacts without any media (zero mediacount)": """
        SELECT m.id, m.title, m.culture, a.mediacount
        FROM artifact_metadata m
        JOIN artifact_media a ON m.id = a.objectid
        WHERE a.mediacount = 0;
    """
    
    
    
}

# Dropdown to select query
selected_query = st.selectbox("Choose a pre-written query:", list(queries.keys()))

# Run Query button
if st.button("Run Query"):
    try:
        conn = get_connection()
        df = pd.read_sql(queries[selected_query], conn)

        # Display results
        st.subheader("📊 Query Results")
        st.dataframe(df)

        # Optional: Simple chart for grouped queries
        if "COUNT" in queries[selected_query].upper():
            st.bar_chart(df.set_index(df.columns[0]))

        conn.close()
    except Error as e:
        st.error(f"❌ Error running query: {e}")














