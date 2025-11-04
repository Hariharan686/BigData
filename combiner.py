"""
SDG 6 — Mapping Access to Clean Water (All-in-One Script)
Author: Hariharan Mini Project
Usage:
  spark-submit sdg6_cleanwater_allinone.py <input_csv> <output_folder>
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, round as sround, expr
import sys, os, json

def main(input_path, output_dir):
    spark = SparkSession.builder.appName("SDG6_CleanWater_AllInOne").getOrCreate()

    # --- 1. Read CSV from HDFS ---
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

    # --- 2. Clean and calculate metrics ---
    df = df.withColumnRenamed("village_name","Village") \
           .withColumnRenamed("district","District") \
           .withColumnRenamed("state","State") \
           .withColumnRenamed("total_households","TotalHouseholds") \
           .withColumnRenamed("households_with_clean_water","CleanWaterHH") \
           .withColumnRenamed("latitude","Lat") \
           .withColumnRenamed("longitude","Lon")

    df = df.withColumn("TotalHouseholds", col("TotalHouseholds").cast("int")) \
           .withColumn("CleanWaterHH", col("CleanWaterHH").cast("int")) \
           .withColumn("Lat", col("Lat").cast("double")) \
           .withColumn("Lon", col("Lon").cast("double"))

    df = df.withColumn("PercentAccess", when(col("TotalHouseholds")>0,
                     (col("CleanWaterHH")/col("TotalHouseholds"))*100).otherwise(0))
    df = df.withColumn("AccessLevel",
                     when(col("PercentAccess")>=75,"High")
                     .when(col("PercentAccess")>=40,"Medium")
                     .otherwise("Low"))

    # --- 3. Save district-level summary ---
    summary = df.groupBy("State","District").agg(
        expr("sum(TotalHouseholds) as TotalHouseholds"),
        expr("sum(CleanWaterHH) as CleanWaterHH")
    )
    summary = summary.withColumn("PercentAccess",
        when(col("TotalHouseholds")>0,
        sround((col("CleanWaterHH")/col("TotalHouseholds"))*100,2)).otherwise(0))
    summary.coalesce(1).write.mode("overwrite").option("header","true").csv(output_dir + "/district_summary")

    # --- 4. Create GeoJSON for map ---
    features = []
    rows = df.select("Village","District","State","Lat","Lon","PercentAccess","AccessLevel").collect()
    for r in rows:
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [r.Lon, r.Lat]},
            "properties": {
                "Village": r.Village,
                "District": r.District,
                "State": r.State,
                "PercentAccess": round(r.PercentAccess,2),
                "AccessLevel": r.AccessLevel
            }
        })
    geojson = {"type": "FeatureCollection", "features": features}

    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir,"points.geojson"),"w") as f:
        json.dump(geojson,f,indent=2)

    # --- 5. Write simple HTML map files ---
    html_code = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>SDG 6 - Clean Water Map</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<link rel="stylesheet" href="styles.css" />
</head>
<body>
<h2>Mapping Access to Clean Water (SDG 6)</h2>
<div id="map"></div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="script.js"></script>
</body>
</html>"""

    css_code = """body {margin:0;font-family:Arial;}
h2 {background:#00796B;color:#fff;margin:0;padding:10px;text-align:center;}
#map {width:100%;height:95vh;}"""

    js_code = """const map = L.map('map').setView([23.5, 80], 6);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {maxZoom:18}).addTo(map);
fetch('points.geojson').then(r=>r.json()).then(data=>{
L.geoJSON(data,{
 pointToLayer:(f,latlng)=>{
   const color=f.properties.AccessLevel==='High'?'green':
                f.properties.AccessLevel==='Medium'?'orange':'red';
   return L.circleMarker(latlng,{radius:6,fillColor:color,color:'#000',weight:0.5,fillOpacity:0.8});
 },
 onEachFeature:(f,layer)=>{
   const p=f.properties;
   layer.bindPopup(`<b>${p.Village}</b><br>${p.District}, ${p.State}<br>${p.PercentAccess}% (${p.AccessLevel})`);
 }
}).addTo(map);
});"""

    with open(os.path.join(output_dir,"index.html"),"w") as f: f.write(html_code)
    with open(os.path.join(output_dir,"styles.css"),"w") as f: f.write(css_code)
    with open(os.path.join(output_dir,"script.js"),"w") as f: f.write(js_code)

    print("✅ Analysis complete.")
    print("Outputs generated in folder:", output_dir)
    print("Files:")
    print(" - district_summary/ (CSV folder)")
    print(" - points.geojson")
    print(" - index.html, styles.css, script.js (open index.html to view map)")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv)!=3:
        print("Usage: spark-submit sdg6_cleanwater_allinone.py <input_csv> <output_folder>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
