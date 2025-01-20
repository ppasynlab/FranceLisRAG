import os
import json
from datetime import datetime
import numpy as np
from sentence_transformers import SentenceTransformer
import configparser
import re
from unidecode import unidecode
from tqdm import tqdm

class MilvusVectorizer:
    def __init__(self, model_name='paraphrase-multilingual-MiniLM-L12-v2'):
        """
        Initialise le modèle de vectorisation.
        
        Args:
            model_name (str): Nom du modèle SentenceTransformer
        """
        try:
            self.model = SentenceTransformer(model_name)
            print(f"Modèle {model_name} chargé avec succès.")
        except Exception as e:
            print(f"Erreur de chargement du modèle : {e}")
            self.model = None
    
    def vectorize(self, text):
        """
        Vectorise un texte.
        
        Args:
            text (str): Texte à vectoriser
        
        Returns:
            list: Vecteur de 256 dimensions
        """
        if self.model is None:
            raise ValueError("Modèle de vectorisation non initialisé")
        
        # Vectoriser le texte
        vector = self.model.encode(text, convert_to_tensor=False)
        
        # Normaliser et tronquer/étendre à 256 dimensions
        if len(vector) > 256:
            vector = vector[:256]
        elif len(vector) < 256:
            vector = np.pad(vector, (0, 256 - len(vector)), 'constant')
        
        return vector.tolist()    

def load_medical_synonyms(config_file='config.ini'):
    config = configparser.ConfigParser()
    config.read(config_file)
    synonyms = {}
    if 'MEDICAL_SYNONYMS' in config:
        for key, value in config['MEDICAL_SYNONYMS'].items():
            synonyms[key] = value.split(',')
    return synonyms

def standardize_label(label: str, medical_synonyms: dict) -> str:
    label = unidecode(label.lower())
    label = re.sub(r'\s+', '-', label)
    label = re.sub(r'[-_]', '-', label)
    label = re.sub(r'[^\w-]', '', label)
    label = re.sub(r'-+', '-', label)
    label = label.strip('-')
    
    for canonical, synonyms in medical_synonyms.items():
        if label in synonyms or label == canonical:
            return canonical
    
    return label

def parse_hl7_to_milvus(file_path, vectorizer):
    """
    Parse un fichier HL7 et convertit les données au format Milvus.
    
    Args:
        file_path (str): Chemin du fichier HL7
        vectorizer (MilvusVectorizer): Objet de vectorisation
    
    Returns:
        dict: Dictionnaire au format Milvus
    """
    try:
        # Lire le contenu du fichier
        with open(file_path, 'r', encoding='utf-8') as fichier:
            file_content = fichier.read()
        
        # Diviser le fichier en lignes
        lines = file_content.split('\n')
        
        # Liste pour stocker les données Milvus
        milvus_data = {
            "collectionName": "FRLISNAQ",  # Nom de collection par défaut
            "data": []
        }
        
        # Parcourir les lignes
        # for i in range(len(lines)):
        for i in tqdm(range(len(lines)), desc="Traitement des lignes", unit="ligne"):
            # Chercher les segments MFE
            if lines[i].startswith('RES'):
                # Diviser le segment MFE par le séparateur |
                resline = lines[i][4:].strip().split('|')
                
                # Vérifier que le segment MFE a suffisamment de champs
                if len(resline) >= 2:
                    # Récupérer le champ Iata_code (2ème partie du champ 5)
                    code_ana = resline[0].strip()
                    libelle_ana = resline[1].strip()
                    chap_ana = resline[2].strip()
                    iata_code = resline[3].strip()
                    
                    medical_synonyms = load_medical_synonyms('config.ini')
                    
                    # Vectoriser le libellé
                    standized_label = standardize_label(libelle_ana, medical_synonyms)
                    
                    try:
                        vector = vectorizer.vectorize(standized_label)
                    except Exception as e:
                        print(f"Erreur de vectorisation pour {libelle_ana}: {e}")
                        vector = [0.0] * 256  # Vecteur nul en cas d'erreur
                    
                    # Ajouter les données au format Milvus
                    milvus_data["data"].append({
                        "Code_Ana": code_ana,
                        "Libelle_Ana": libelle_ana,
                        "Libelle_Llm": standized_label,
                        "Iata_code": iata_code,
                        "Chap_Ana": chap_ana,
                        "vector": vector
                    })
        
        return milvus_data
    
    except FileNotFoundError:
        print(f"Erreur : Le fichier {file_path} n'a pas été trouvé.")
        return None
    except Exception as e:
        print(f"Une erreur s'est produite : {e}")
        return None

def exporter_milvus_json(milvus_data, dossier_export=None):
    """
    Exporte les données Milvus au format JSON.
    
    Args:
        milvus_data (dict): Données au format Milvus
        dossier_export (str, optional): Dossier de destination. 
                                        Si None, utilise le dossier courant.
    
    Returns:
        str: Chemin complet du fichier exporté
    """
    if not milvus_data:
        print("Aucune donnée à exporter.")
        return None
    
    # Générer le nom du fichier
    horodatage = datetime.now().strftime('%Y%m%d_%H%M%S')
    nom_fichier = f"milvus_export_{horodatage}.json"
    
    # Déterminer le chemin complet du fichier
    if dossier_export:
        # Créer le dossier s'il n'existe pas
        os.makedirs(dossier_export, exist_ok=True)
        chemin_export = os.path.join(dossier_export, nom_fichier)
    else:
        # Utiliser le dossier courant
        chemin_export = nom_fichier
    
    # Exporter les données
    with open(chemin_export, 'w', encoding='utf-8') as fichier_export:
        json.dump(milvus_data, fichier_export, indent=2, ensure_ascii=False)
    
    print(f"Données Milvus exportées dans : {chemin_export}")
    return chemin_export

def display_vector_info(json_data):
    """
    Affiche le nombre total de vecteurs et leur longueur unique à partir des données JSON.

    Args:
        json_data (dict): Les données JSON contenant les vecteurs.
    """
    if 'data' not in json_data or not isinstance(json_data['data'], list):
        print("Aucune donnée de vecteur trouvée.")
        return

    vector_length_count = {}
    total_vectors = 0

    for item in json_data['data']:
        if 'vector' in item:
            vector = item['vector']
            vector_length = len(vector)
            total_vectors += 1
            
            if vector_length in vector_length_count:
                vector_length_count[vector_length] += 1
            else:
                vector_length_count[vector_length] = 1

    # Affichage des résultats
    print(f"\nNombre total de vecteurs: {total_vectors}")
    for length, count in vector_length_count.items():
        print(f"{count} vecteurs avec une longueur de {length}")

# Exemple de requête de recherche
def rechercher_code_ana(libelle_recherche, milvus_data):
    """
    Recherche les codes Ana correspondant à un libellé dans les données Milvus.
    
    Args:
        libelle_recherche (str): Libellé à rechercher
        milvus_data (dict): Données Milvus
    
    Returns:
        list: Liste des codes Ana correspondants
    """
    vectorizer = MilvusVectorizer()
    vector_recherche = vectorizer.vectorize(libelle_recherche)
    
    resultats = []
    for entry in milvus_data['data']:
        # Calculer la similarité cosinus entre les vecteurs
        similarite = np.dot(vector_recherche, entry['vector']) / (
            np.linalg.norm(vector_recherche) * np.linalg.norm(entry['vector'])
        )
        
        # Seuil de similarité (à ajuster)
        if similarite > 0.8:
            resultats.append({
                'Code_Ana': entry['Code_Ana'],
                'Libelle_Ana': entry['Libelle_Ana'],
                'Similarite': similarite
            })
    
    # Trier par similarité décroissante
    return sorted(resultats, key=lambda x: x['Similarite'], reverse=True)

# Exemple d'utilisation
if __name__ == "__main__":
    # Initialiser le vectoriseur
    vectorizer = MilvusVectorizer()
    
    # Chemin du fichier HL7
    chemin_fichier_hl7 = r'C:\Users\patrick.paysan\Documents\VisualStudioCode\PPALAO-FRANCELIS-RAG\export_lib_20250120_145032.txt'
    
    # Timer pour mesurer le temps d'exécution
    start_time = datetime.now()
    
    print("Début du traitement.")
    
    # Parser le fichier HL7
    donnees_milvus = parse_hl7_to_milvus(chemin_fichier_hl7, vectorizer)
    
    # Exporter au format JSON Milvus
    if donnees_milvus:
        # Export dans le dossier courant
        chemin_export = exporter_milvus_json(donnees_milvus)
        
                # Afficher le temps d'exécution
        execution_time = datetime.now() - start_time
        print(f"\nTemps total d'exécution: {execution_time}")
        print(f"Nombre d'analyses traitées: {len(donnees_milvus['data'])}")
        
        # Supposons que vous ayez déjà chargé vos données JSON dans une variable `milvus_data`
        # Vous pouvez appeler la fonction ici
        display_vector_info(donnees_milvus)

        medical_synonyms = load_medical_synonyms('config.ini')        
        # Exemple de recherche
        libelle_test = "DETERMINATION DU GROUPE SANGUIN"
        standardized_libelle_test=standardize_label(libelle_test, medical_synonyms)
        
        resultats = rechercher_code_ana(standardized_libelle_test, donnees_milvus)
        
        print(f"\nRésultats pour '{libelle_test}':")
        for resultat in resultats:
            print(f"Code Ana: {resultat['Code_Ana']}, "
                  f"Libellé: {resultat['Libelle_Ana']}, "
                  f"Similarité: {resultat['Similarite']:.4f}")