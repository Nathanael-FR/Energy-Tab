# Utiliser une image de base avec Python
FROM python:3.9

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Copier les fichiers requis dans le conteneur
COPY requirements.txt .
COPY scripts/ ./scripts
COPY data/ ./data

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Commande par défaut pour exécuter le script Python
CMD ["python", "./scripts/load.py"]
