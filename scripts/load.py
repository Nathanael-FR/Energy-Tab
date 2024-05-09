import psycopg2


if __name__ == "__main__":
    
    conn = psycopg2.connect(
    host="postgres",
    port="5432",
    database="project",
    user="root",
    password="password"
)

    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("Version de PostgreSQL :", record)

    # Fermer la connexion
    cursor.close()
    conn.close()
