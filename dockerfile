# Utiliser une image de base avec Python
FROM bitnami/spark:3.5.1

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Copier les fichiers requis dans le conteneur
COPY requirements.txt .
COPY scripts/ ./scripts
COPY data/ ./data

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Commande par défaut pour exécuter le script Python
CMD ["spark-submit", "--master", "local", "./main.py"]
