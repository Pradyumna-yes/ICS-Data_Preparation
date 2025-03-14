import pyodbc
conn_str = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=<CRMServer3>;DATABASE=<CRM_MSCRM>"
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
cursor.execute("SELECT @@VERSION")
print(cursor.fetchone())