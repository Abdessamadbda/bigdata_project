import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

def load_data_from_output(start_date, end_date):
    """
    Charger les données depuis les fichiers dans le répertoire output en fonction des dates spécifiées.
    Les fichiers doivent être nommés selon le format YYYYMMDDHH.
    """
    output_dir = "./output"
    
    files = os.listdir(output_dir)
    
    data_frames = []
    
    for file in files:
        try:
            file_hour = file.split(".")[0]
            file_date = datetime.strptime(file_hour, "%Y%m%d%H")
            
            if start_date <= file_date <= end_date:
                file_path = os.path.join(output_dir, file)
                df = pd.read_csv(file_path, delimiter="|", names=["formatted_date", "article", "total_sales"])
                data_frames.append(df)
        except Exception as e:
            print(f"Erreur lors de la lecture du fichier {file}: {str(e)}")
    
    if data_frames:
        return pd.concat(data_frames, ignore_index=True)
    else:
        print("Aucune donnée trouvée pour la période spécifiée.")
        return None

def display_dashboard(data):
    """
    Afficher le dashboard avec des visualisations de données.
    """
    if data is None or data.empty:
        print("Aucune donnée à afficher.")
        return
    
    data['formatted_date'] = pd.to_datetime(data['formatted_date'], format='%Y/%m/%d %H')
    
    aggregated_data = data.groupby(['formatted_date', 'article'])['total_sales'].sum().reset_index()

    plt.figure(figsize=(10, 6))
    aggregated_data.groupby('article')['total_sales'].sum().sort_values(ascending=False).plot(kind='bar')
    plt.title('Ventes Totales par Article')
    plt.xlabel('Article')
    plt.ylabel('Total des Ventes')
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.show()

    plt.figure(figsize=(10, 6))
    aggregated_data.groupby('formatted_date')['total_sales'].sum().plot(kind='line')
    plt.title('Evolution des Ventes au Fil du Temps')
    plt.xlabel('Date')
    plt.ylabel('Total des Ventes')
    plt.tight_layout()
    plt.show()

def main():
    if len(sys.argv) != 3:
        print("Usage: python dashboard.py <start_date> <end_date>")
        print("<start_date> et <end_date> doivent être au format YMD (ex: 20241119).")
        sys.exit(1)
    
    start_date_str = sys.argv[1]
    end_date_str = sys.argv[2]

    try:
        start_date = datetime.strptime(start_date_str, "%Y%m%d")
        end_date = datetime.strptime(end_date_str, "%Y%m%d")
    except ValueError:
        print("Erreur : les dates doivent être au format YMD (ex: 20241119).")
        sys.exit(1)

    data = load_data_from_output(start_date, end_date)

    display_dashboard(data)

if __name__ == "__main__":
    main()
