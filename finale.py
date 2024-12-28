# -*- coding: utf-8 -*-
"""
Created on Sat Dec 28 11:38:20 2024

@author: DELL
"""
import requests

import pandas as pd
import sqlite3

import streamlit as st

# Fonction de connexion à la base de données SQLite




import os


# Fonction pour télécharger la base de données depuis Google Drive
def download_db_from_drive():
    # Lien direct de téléchargement (remplacez par votre lien Drive)
    url = "https://drive.google.com/uc?id=1eFejDWf48IAbhj1NpxasApB2fHKmKCYC&export=download"
    local_path = "Bd_projet.db"  # Nom du fichier local temporaire

    try:
        # Télécharger le fichier
        response = requests.get(url)
        response.raise_for_status()  # Vérifie les erreurs HTTP
        with open(local_path, 'wb') as file:
            file.write(response.content)
        
        return local_path
    except requests.exceptions.RequestException as e:
        
        return None

# Fonction de connexion à la base de données SQLite
def connect_to_db():
    # Télécharger la base depuis Google Drive
    db_path = download_db_from_drive()
    if db_path and os.path.exists(db_path):  # Vérifier si le fichier est téléchargé
        try:
            # Établir la connexion à la base SQLite
            connection = sqlite3.connect(db_path)
            
            return connection
        except sqlite3.Error as e:
            
            return None
    else:
        
        return None





# Fonction pour récupérer les données d'une table
def fetch_table_data(table_name, where_clause=None, limit=None):
    connection = connect_to_db()
    cursor = connection.cursor()
    query = f"SELECT * FROM {table_name}"
    if where_clause:
        query += f" WHERE {where_clause}"
    if limit:
        query += f" LIMIT {limit}"
    cursor.execute(query)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    connection.close()
    return pd.DataFrame(data, columns=columns)

# Fonction pour vérifier si un identifiant existe
def check_if_id_exists(table_name, column, value):
    connection = connect_to_db()
    cursor = connection.cursor()
    query = f"SELECT COUNT(*) FROM {table_name} WHERE {column} = ?"
    cursor.execute(query, (value,))
    exists = cursor.fetchone()[0] > 0
    connection.close()
    return exists

# Fonction pour insérer des données
def insert_data(table_name, columns, values):
    connection = connect_to_db()
    cursor = connection.cursor()
    placeholders = ", ".join("?" for _ in columns)
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    cursor.execute(query, values)
    connection.commit()
    connection.close()

# Fonction pour mettre à jour des données
def update_data(table_name, primary_key_column, primary_key_value, updates):
    connection = connect_to_db()
    cursor = connection.cursor()
    set_clause = ", ".join(f"{col} = ?" for col in updates.keys())
    query = f"UPDATE {table_name} SET {set_clause} WHERE {primary_key_column} = ?"
    cursor.execute(query, list(updates.values()) + [primary_key_value])
    connection.commit()
    connection.close()

# Fonction pour supprimer des données
def delete_data(table_name, column, value):
    connection = connect_to_db()
    cursor = connection.cursor()
    query = f"DELETE FROM {table_name} WHERE {column} = ?"
    cursor.execute(query, (value,))
    connection.commit()
    connection.close()

# Fonction pour exécuter une requête SQL personnalisée
def execute_custom_query(query):
    connection = connect_to_db()
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        connection.close()
        return pd.DataFrame(data, columns=columns)
    except sqlite3.Error as e:
        connection.close()
        return f"Erreur SQL: {e}"
def execute_predefined_query(query):
    connection = connect_to_db()
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        connection.close()
        return pd.DataFrame(data, columns=columns)
    except sqlite3.Error as e:
        connection.close()
        return f"Erreur SQL: {e}"
def find_merge_column(df1, df2):
    common_columns = list(set(df1.columns) & set(df2.columns))
    if common_columns:
        return common_columns[0]  # Retourne la première colonne commune trouvée
    return None  # Si aucune colonne commune n'est trouvée

# Fusionner deux tables
def merge_tables(table_1, table_2):
    df1 = fetch_table_data(table_1)
    df2 = fetch_table_data(table_2)

    merge_column = find_merge_column(df1, df2)
    
    if merge_column:
        merged_data = pd.merge(df1, df2, on=merge_column, how='inner')
        return merged_data, merge_column
    else:
        return None, None  # Aucune clé de fusion trouvée
# Fonction principale
def main():
    # Utilisation de Markdown pour personnaliser l'apparence
    st.markdown("""
        <style>
            .stApp {
                background-color: #f4f4f9;
            }
            .sidebar .sidebar-content {
                background-color: #006D77;
                color: white;
            }
            .sidebar .sidebar-content .css-1p4tqxn {
                background-color: #008C92;
            }
            .css-1x8jfvs {
                font-size: 18px;
                font-weight: bold;
                color: #005f73;
            }
            .stButton>button {
                background-color: #008C92;
                color: white;
            }
            .stButton>button:hover {
                background-color: #006D77;
            }
            .stSelectbox, .stMultiselect, .stTextInput, .stNumberInput {
                background-color: #ffffff;
                color: #333;
            }
            .sidebar .stRadio label {
                font-size: 20px; /* Augmenter la taille de la police */
                color: #FF6347;  /* Changer la couleur du texte */
            }
            .sidebar .stRadio button {
                background-color: #f0f0f0; /* Couleur de fond du bouton */
                border-radius: 5px; /* Bordure arrondie */
            }
            .sidebar .stRadio:hover label {
                color: #008CBA; /* Changer la couleur au survol */
            }
        </style>
    """, unsafe_allow_html=True)

    st.title("Gestion de la base de données")

    # Barre latérale avec des couleurs de fond et textes ajustés
    st.sidebar.header("Actions")
    action = st.sidebar.radio(
        "Choisir une action", 
        ["Afficher les données", "Ajouter des données", "Modifier des données", "Supprimer des données", "Requete demande dans le projet"]
    )
   
    # Sélection de la table
    tables = ["Etudiant", "Contact", "Nationalite", "Note", "Matiere"]
    selected_table = st.selectbox("Choisir une table", tables)

    # Affichage de données
    if action == "Afficher les données":
        st.subheader(f"Données de la table {selected_table}")
        data = fetch_table_data(selected_table)
        st.dataframe(data, use_container_width=True)

        # **Filtrage par plusieurs colonnes**
        st.subheader("Filtrer les données par plusieurs colonnes")
        selected_columns = st.multiselect("Choisissez les colonnes pour filtrer", data.columns)
        filters = {}
        if selected_columns:
            for col in selected_columns:
                value = st.text_input(f"Entrez la valeur pour {col}", key=f"filter_{col}")
                if value:
                    filters[col] = value

        where_clauses = [f"{col} = '{val}'" for col, val in filters.items()]
        where_clause = " AND ".join(where_clauses)

        if st.button("Appliquer le filtre"):
            try:
                filtered_data = fetch_table_data(selected_table, where_clause=where_clause)
                st.dataframe(filtered_data)
            except Exception as e:
                st.error(f"Erreur dans le filtre : {e}")

        # Sélectionner la colonne de fusion (clé primaire ou autre)
       # Sélectionner la colonne de fusion automatiquement
        st.subheader("Sélectionner à travers deux bases de données")
        table_1 = st.selectbox("Choisissez la première table", tables)
        table_2 = st.selectbox("Choisissez la deuxième table", tables)

        if st.button("Fusionner les bases de données"):
            try:
                merged_data, merge_column = merge_tables(table_1, table_2)
                if merged_data is not None:
                    st.success(f"Les données ont été fusionnées avec succès sur la colonne '{merge_column}'")
                    st.dataframe(merged_data)
                else:
                    st.error("Aucune colonne commune n'a été trouvée pour la fusion.")
            except Exception as e:
                st.error(f"Erreur lors de la fusion : {e}")

    # Ajouter des données
    elif action == "Ajouter des données":
        st.subheader(f"Ajouter des données à la table {selected_table}")
        data = fetch_table_data(selected_table)
        primary_key_column = data.columns[0]

        with st.form("add_form"):
            new_values = []
            new_primary_key_value = st.text_input(f"Valeur pour {primary_key_column}")

            for col in data.columns[1:]:
                new_value = st.text_input(f"Valeur pour {col}", key=f"value_{col}")
                new_values.append(new_value)

            add_button = st.form_submit_button("Ajouter")

            if add_button:
                if not new_primary_key_value:
                    st.error("La clé primaire est obligatoire.")
                elif check_if_id_exists(selected_table, primary_key_column, new_primary_key_value):
                    st.error(f"L'identifiant {new_primary_key_value} existe déjà.")
                else:
                    try:
                        new_values.insert(0, new_primary_key_value)
                        insert_data(selected_table, data.columns, new_values)
                        st.success(f"Données ajoutées avec succès dans la table {selected_table} !")
                    except Exception as e:
                        st.error(f"Une erreur est survenue : {e}")

    # Modifier des données
    elif action == "Modifier des données":
        st.subheader(f"Modifier des données dans la table {selected_table}")
        data = fetch_table_data(selected_table)
        primary_key_column = data.columns[0]

        primary_key_value = st.text_input(f"Entrez la valeur de {primary_key_column} pour modifier l'entrée")
        if st.button("Chercher"):
            if not check_if_id_exists(selected_table, primary_key_column, primary_key_value):
                st.error(f"Aucune entrée trouvée avec {primary_key_column} = {primary_key_value}")
            else:
                current_data = fetch_table_data(selected_table, where_clause=f"{primary_key_column} = {primary_key_value}")
                st.dataframe(current_data)

                updates = {}
                for col in data.columns[1:]:
                    new_value = st.text_input(f"Nouvelle valeur pour {col}")
                    if new_value:
                        updates[col] = new_value

                if updates:
                    if st.button("Mettre à jour"):
                        try:
                            update_data(selected_table, primary_key_column, primary_key_value, updates)
                            st.success("Données mises à jour avec succès.")
                        except Exception as e:
                            st.error(f"Erreur lors de la mise à jour des données : {e}")
        
    # Supprimer des données
    elif action == "Supprimer des données":
        st.subheader(f"Supprimer des données de la table {selected_table}")
        data = fetch_table_data(selected_table)
        primary_key_column = data.columns[0]

        primary_key_value = st.text_input(f"Entrez l'identifiant pour supprimer l'entrée de {primary_key_column}")
        if st.button("Supprimer"):
            if not check_if_id_exists(selected_table, primary_key_column, primary_key_value):
                st.error(f"Aucune entrée trouvée avec {primary_key_column} = {primary_key_value}")
            else:
                delete_data(selected_table, primary_key_column, primary_key_value)
                st.success(f"Données supprimées avec succès.")
                
    # Exécuter une requête personnalisée
    elif action == "Requete demande dans le projet":
        st.subheader("Exécuter une requête prédéfinie")

        query_options = [
        "Affichage des mentions",
        "la moyenne des notes d'un étudiant par matiere",
        "b.	Ordonner les étudiants par note",
        "Afficher les étudiants ayant  la moyenne",
        "Moyenne par classe"
    ]
    
        selected_query = st.selectbox("Choisissez une requête", query_options)
    
        if selected_query == query_options[0]:  # "Affichage des mentions"
            
            
                query = f"""
                SELECT 
    E.matricule,
    E.nom,
    E.prenom,
    AVG((2 * N.examen + N.controle) / 3) AS moyenne,
    CASE 
        WHEN AVG((2 * N.examen + N.controle) / 3) >= 16 THEN 'Très Bien'
        WHEN AVG((2 * N.examen + N.controle) / 3) >= 14 THEN 'Bien'
        WHEN AVG((2 * N.examen + N.controle) / 3) >= 12 THEN 'Assez Bien'
        WHEN AVG((2 * N.examen + N.controle) / 3) >= 10 THEN 'Passable'
        ELSE 'Insuffisant'
    END AS mention
FROM 
    Note N
JOIN
    Etudiant E ON N.matricule = E.matricule
GROUP BY 
    E.matricule, E.nom, E.prenom
ORDER BY 
    moyenne DESC;
                """
                result = execute_predefined_query(query)
                st.dataframe(result)
    
        elif selected_query == query_options[1]:  # "la moyenne des notes d'un étudiant"
            
                query = f"""
            SELECT 
    E.matricule,
    E.nom,
    E.prenom,
    ((2 * N.examen + N.controle) / 3) AS moyenne,M.nom_matiere
FROM 
    Note N
JOIN
    Etudiant E ON N.matricule = E.matricule
JOIN
    matiere M ON N.id_matiere = M.id_matiere
GROUP BY 
    E.matricule,E.nom, E.prenom,M.nom_matiere
ORDER BY 
    moyenne DESC,M.nom_matiere;
            """
                result = execute_predefined_query(query)
                st.dataframe(result)
    
        elif selected_query == query_options[2]:  # "Lister les étudiants inscrits"
            
            query = f"""
            SELECT 
        E.matricule,
        E.nom,
        E.prenom,
        AVG((2 * N.examen + N.controle) / 3) AS moyenne
    FROM 
        Note N
    JOIN
        Etudiant E ON N.matricule = E.matricule
    GROUP BY 
        E.matricule, E.nom, E.prenom
    ORDER BY 
        moyenne DESC;
            """
            result = execute_predefined_query(query)
            st.dataframe(result)
    
        elif selected_query == query_options[3]:  # "Afficher les étudiants ayant  la moyenne"
            
            query = f"""
        SELECT 
    E.matricule,
    E.nom,
    E.prenom,
    AVG((2 * N.examen + N.controle) / 3) AS moyenne
FROM 
    Note N
JOIN
    Etudiant E ON N.matricule = E.matricule
GROUP BY 
    E.matricule, E.nom, E.prenom
HAVING 
    moyenne >= 10
ORDER BY 
    moyenne DESC;
        """
            result = execute_predefined_query(query)
            st.dataframe(result)
    
        elif selected_query == query_options[4]:  # "Moyenne par classes"
            query = """
        SELECT 
    C.nom_classe,
    AVG((2 * N.examen + N.controle) / 3) AS moyenne_classe
FROM 
    Note N
JOIN
    Etudiant E ON N.matricule = E.matricule
JOIN
    Classe C ON E.id_classe = C.id_classe
GROUP BY 
    C.nom_classe;
        """
            result = execute_predefined_query(query)
            st.dataframe(result)
if __name__ == "__main__":
    main()
