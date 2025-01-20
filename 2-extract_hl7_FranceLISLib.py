import os
from datetime import datetime

def generer_nom_fichier_export(prefixe='export_lib', extension='txt'):
    """
    Génère un nom de fichier avec horodatage.
    
    Args:
        prefixe (str): Préfixe du nom de fichier
        extension (str): Extension du fichier
    
    Returns:
        str: Nom de fichier horodaté
    """
    horodatage = datetime.now().strftime('%Y%m%d_%H%M%S')
    nom_fichier = f"{prefixe}_{horodatage}.{extension}"
    return nom_fichier

def extraire_sous_champs(champ):
    """
    Extrait les sous-champs d'un champ HL7.
    
    Args:
        champ (str): Chaîne contenant les sous-champs
    
    Returns:
        list: Liste des sous-champs
    """
    return champ.split('^')

def parse_hl7_file(file_path):
    """
    Extrait les champs spécifiques des segments MFE et OM1 et analyse leurs longueurs.
    Élimine les doublons basés sur la ligne RES et filtre les entrées où sous_champ3 != "ADMINISTRATIF".
    
    Args:
        file_path (str): Chemin du fichier HL7
    
    Returns:
        tuple: (Liste de dictionnaires contenant les champs extraits,
               Dictionnaire contenant les longueurs maximales des sous-champs,
               Statistiques sur les doublons)
    """
    try:
        max_lengths = {
            'OM1_sous_champ1': 0,
            'OM1_sous_champ2': 0,
            'OM1_sous_champ3': 0,
            'MFE5_sous_champ2': 0
        }
        
        res_uniques = set()
        results = []
        nb_doublons = 0
        sous_champ3_values = set()  # Pour stocker les valeurs uniques de sous_champ3
        
        with open(file_path, 'r', encoding='utf-8') as fichier:
            file_content = fichier.read()
        
        lines = file_content.split('\n')
        
        for i in range(len(lines)):
            if lines[i].startswith('MFE'):
                mfe_fields = lines[i].split('|')
                
                if len(mfe_fields) >= 5:
                    champ5 = mfe_fields[4]
                    mfe_champ5_parts = extraire_sous_champs(champ5)
                    mfe_champ5_sous_champ2 = mfe_champ5_parts[1] if len(mfe_champ5_parts) > 1 else ""
                    
                    max_lengths['MFE5_sous_champ2'] = max(max_lengths['MFE5_sous_champ2'], 
                                                        len(mfe_champ5_sous_champ2))
                    
                    if i+1 < len(lines) and lines[i+1].startswith('OM1'):
                        om1_fields = lines[i+1].split('|')
                        
                        if len(om1_fields) >= 3:
                            champ3_om1 = om1_fields[2]
                            om1_champ3_parts = extraire_sous_champs(champ3_om1)
                            
                            sous_champ1 = om1_champ3_parts[0] if len(om1_champ3_parts) > 0 else ""
                            sous_champ2 = om1_champ3_parts[1] if len(om1_champ3_parts) > 1 else ""
                            sous_champ3 = om1_champ3_parts[2] if len(om1_champ3_parts) > 2 else ""
                            
                            # Ajouter la valeur à l'ensemble des sous_champ3 uniques
                            sous_champ3_values.add(sous_champ3)
                            
                            # Vérifier si sous_champ3 n'est pas "ADMINISTRATIF"
                            if sous_champ3 != "ADMINISTRATIF" and sous_champ3 != "CONCLUSION":
                                max_lengths['OM1_sous_champ1'] = max(max_lengths['OM1_sous_champ1'], 
                                                                   len(sous_champ1))
                                max_lengths['OM1_sous_champ2'] = max(max_lengths['OM1_sous_champ2'], 
                                                                   len(sous_champ2))
                                max_lengths['OM1_sous_champ3'] = max(max_lengths['OM1_sous_champ3'], 
                                                                   len(sous_champ3))
                                
                                res_line = f"{sous_champ1}|{sous_champ2}|{sous_champ3}|{mfe_champ5_sous_champ2}"
                                
                                if res_line not in res_uniques:
                                    res_uniques.add(res_line)
                                    results.append({
                                        'MFE_champ4': mfe_fields[3],
                                        'MFE_champ5': champ5,
                                        'OM1_champ3': champ3_om1,
                                        'RES': res_line,
                                        'sous_champs': {
                                            'OM1_sous_champ1': sous_champ1,
                                            'OM1_sous_champ2': sous_champ2,
                                            'OM1_sous_champ3': sous_champ3,
                                            'MFE5_sous_champ2': mfe_champ5_sous_champ2
                                        }
                                    })
                                else:
                                    nb_doublons += 1
        
        # Log des valeurs uniques de sous_champ3
        print("\nValeurs uniques de sous_champ3 rencontrées :")
        print("-" * 40)
        for valeur in sorted(sous_champ3_values):
            print(f"- {valeur}")
        print("-" * 40)
        
        return results, max_lengths, {'nb_doublons': nb_doublons, 'nb_uniques': len(results)}
    
    except FileNotFoundError:
        print(f"Erreur : Le fichier {file_path} n'a pas été trouvé.")
        return [], {}, {'nb_doublons': 0, 'nb_uniques': 0}
    except PermissionError:
        print(f"Erreur : Permission refusée pour accéder au fichier {file_path}.")
        return [], {}, {'nb_doublons': 0, 'nb_uniques': 0}
    except Exception as e:
        print(f"Une erreur s'est produite lors de la lecture du fichier : {e}")
        return [], {}, {'nb_doublons': 0, 'nb_uniques': 0}

def afficher_longueurs_maximales(max_lengths):
    """
    Affiche les longueurs maximales des sous-champs extraits.
    
    Args:
        max_lengths (dict): Dictionnaire contenant les longueurs maximales
    """
    print("\nLongueurs maximales des sous-champs :")
    print("-" * 40)
    print(f"OM1 Sous-champ 1: {max_lengths['OM1_sous_champ1']} caractères")
    print(f"OM1 Sous-champ 2: {max_lengths['OM1_sous_champ2']} caractères")
    print(f"OM1 Sous-champ 3: {max_lengths['OM1_sous_champ3']} caractères")
    print(f"MFE5 Sous-champ 2: {max_lengths['MFE5_sous_champ2']} caractères")
    print("-" * 40)

def afficher_statistiques_doublons(stats):
    """
    Affiche les statistiques sur les doublons.
    
    Args:
        stats (dict): Dictionnaire contenant les statistiques
    """
    print("\nStatistiques d'extraction :")
    print("-" * 40)
    print(f"Nombre d'enregistrements uniques : {stats['nb_uniques']}")
    print(f"Nombre de doublons évités : {stats['nb_doublons']}")
    print("-" * 40)

def exporter_resultats(resultats, dossier_export=None):
    """
    Exporte les résultats dans un fichier horodaté.
    
    Args:
        resultats (list): Liste des résultats à exporter
        dossier_export (str, optional): Dossier de destination. 
                                        Si None, utilise le dossier courant.
    
    Returns:
        str: Chemin complet du fichier exporté
    """
    nom_fichier = generer_nom_fichier_export()
    
    if dossier_export:
        os.makedirs(dossier_export, exist_ok=True)
        chemin_export = os.path.join(dossier_export, nom_fichier)
    else:
        chemin_export = nom_fichier
    
    with open(chemin_export, 'w', encoding='utf-8') as fichier_export:
        for resultat in resultats:
            fichier_export.write(f"MFE Champ 4: {resultat['MFE_champ4']}\n")
            fichier_export.write(f"MFE Champ 5: {resultat['MFE_champ5']}\n")
            fichier_export.write(f"OM1 Champ 3: {resultat['OM1_champ3']}\n")
            fichier_export.write(f"RES: {resultat['RES']}\n")
            fichier_export.write("---\n")
    
    print(f"Résultats exportés dans : {chemin_export}")
    return chemin_export

# Exemple d'utilisation
chemin_fichier = r'C:\Users\patrick.paysan\Documents\Synlab\10-Projets\LAO\LAO-Data\LCSD_NAQ_NAQ-MSPB_20241202004553.HL7\LCSD_NAQ_NAQ-MSPB_20241202004553.HL7'

# Utiliser la fonction de parsing modifiée
resultats, max_lengths, stats = parse_hl7_file(chemin_fichier)

# Afficher les statistiques et les longueurs maximales
if resultats:
    afficher_statistiques_doublons(stats)
    afficher_longueurs_maximales(max_lengths)
    
    # Exporter les résultats
    exporter_resultats(resultats)
else:
    print("Aucun résultat n'a pu être extrait.")