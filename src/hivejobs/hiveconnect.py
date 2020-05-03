import jaydebeapi


database='hive_lab'
driver='org.apache.hive.jdbc.HiveDriver'
url=(f"jdbc:hive2://localhost:10000/{database}")
conn=jaydebeapi.connect("org.apache.hive.jdbc.HiveDriver", url)
cursor = conn.cursor()
sql="select * from employee limit 10"
cursor.execute(sql)
results = cursor.fetchall()
print(results)

