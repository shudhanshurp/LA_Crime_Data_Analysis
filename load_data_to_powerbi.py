import duckdb
con = duckdb.connect(database='D:/VS code/Projects/Data Engineering/LA Data Analysis/data/database.duckdb', read_only=True)
crime_data = con.execute('SELECT * FROM crime_data').fetchdf()
area_summary = con.execute('SELECT * FROM area_summary').fetchdf()
top10crime = con.execute('SELECT * FROM top10crime').fetchdf()