import json
import math
import psutil
import os
from pathlib import Path
from tqdm import tqdm

def get_available_memory():
    """
    Retourne la mémoire disponible en octets
    """
    return psutil.virtual_memory().available

def get_file_size(file_path):
    """
    Retourne la taille du fichier en octets
    """
    return os.path.getsize(file_path)

def check_memory_requirement(file_size):
    """
    Vérifie si la mémoire disponible est suffisante (avec une marge de sécurité de 20%)
    """
    available_memory = get_available_memory()
    required_memory = file_size * 1.2  # 20% de marge de sécurité
    
    if required_memory > available_memory:
        raise MemoryError(
            f"Mémoire insuffisante pour charger le fichier en toute sécurité.\n"
            f"Taille du fichier: {file_size / 1024 / 1024:.2f} MB\n"
            f"Mémoire requise (avec marge): {required_memory / 1024 / 1024:.2f} MB\n"
            f"Mémoire disponible: {available_memory / 1024 / 1024:.2f} MB"
        )

def split_json_file(input_file, num_splits=4):
    """
    Divise un fichier JSON contenant une structure spécifique en plusieurs fichiers de taille égale.
    
    Args:
        input_file (str): Chemin vers le fichier JSON d'entrée
        num_splits (int): Nombre de fichiers à créer
    """
    print("Vérification de la mémoire disponible...")
    file_size = get_file_size(input_file)
    check_memory_requirement(file_size)
    
    print("Lecture du fichier JSON...")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if not isinstance(data, dict) or 'data' not in data or not isinstance(data['data'], list):
        raise ValueError("Le fichier JSON doit contenir un objet avec une clé 'data' qui est une liste d'objets")
    
    total_items = len(data['data'])
    items_per_split = math.ceil(total_items / num_splits)
    
    input_path = Path(input_file)
    output_dir = input_path.parent / f"{input_path.stem}_splits"
    output_dir.mkdir(exist_ok=True)
    
    print(f"\nCréation de {num_splits} fichiers...")
    with tqdm(total=total_items, desc="Progression") as pbar:
        for i in range(num_splits):
            start_idx = i * items_per_split
            end_idx = min((i + 1) * items_per_split, total_items)
            
            if start_idx >= total_items:
                break
            
            split_data = data['data'][start_idx:end_idx]
            output_file = output_dir / f"{input_path.stem}_{i+1}.json"
            
            # Création de la nouvelle structure
            output_json = {
                "collectionName": data.get("collectionName", "FRANCELIS"),
                "data": split_data
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output_json, f, ensure_ascii=False, indent=2)
            
            # Mise à jour de la barre de progression
            pbar.update(len(split_data))
            
            # Affichage des informations pour chaque fichier
            file_size_mb = os.path.getsize(output_file) / 1024 / 1024
            print(f"\nFichier {i+1}/{num_splits} créé : {output_file}")
            print(f"Nombre d'éléments : {len(split_data)}")
            print(f"Taille du fichier : {file_size_mb:.2f} MB")

    print("\nDécoupage terminé avec succès!")

# Exemple d'utilisation
if __name__ == "__main__":
    input_file = r"C:\Users\patrick.paysan\Documents\Synlab\10-Projets\LAO\LAO-Data\RAG_FRLIS\milvus_export_20250120_172832.json"
    try:
        split_json_file(input_file)
    except MemoryError as e:
        print(f"Erreur de mémoire : {e}")
    except Exception as e:
        print(f"Une erreur est survenue : {e}")