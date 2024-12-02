import csv
import time
import pandas as pd
import random 
from pymongo import MongoClient
from bson.objectid import ObjectId

def create_players_collection(db):
    player_schema = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["player_id", "name", "position", "team", "start_year"],
            "properties": {
                # Información Personal
                "player_id": {"bsonType": "string"},
                "name": {"bsonType": "string"},
                "nationality": {"bsonType": ["string", "null"]},
                "position": {"bsonType": "string"},
                "age": {"bsonType": ["int", "null"]},
                "birth_year": {"bsonType": ["int", "null"]},
                "start_year": { "bsonType": "int" },
                # Información del Equipo
                "team": {
                    "bsonType": "object",
                    "required": ["squad_id", "name"],
                    "properties": {
                        "squad_id": {"bsonType": "string"},
                        "name": {"bsonType": "string"}
                    }
                },
                # Estadísticas Básicas
                "stats": {
                    "bsonType": "object",
                    "properties": {
                        "matches_played": {"bsonType": "int"},            # MP
                        "starts": {"bsonType": "int"},                    # Starts
                        "minutes_played": {"bsonType": "int"},            # Min
                        "goals": {"bsonType": "int"},                     # Gls
                        "assists": {"bsonType": "int"},                   # Ast
                        "goals_minus_pks": {"bsonType": "int"},           # G-PK
                        "penalty_goals": {"bsonType": "int"},             # PK
                        "penalty_attempts": {"bsonType": "int"},          # PKatt
                        "yellow_cards": {"bsonType": "int"},              # CrdY
                        "red_cards": {"bsonType": "int"},                 # CrdR
                        "second_yellow_cards": {"bsonType": "int"},       # 2CrdY
                    }
                },
                # Estadísticas Avanzadas
                "advanced_stats": {
                    "bsonType": "object",
                    "properties": {
                        "xG": {"bsonType": "double"},                     # xG
                        "npxG": {"bsonType": "double"},                   # npxG
                        "xAG": {"bsonType": "double"},                    # xAG
                        "shots": {"bsonType": "int"},                     # Sh
                        "shots_on_target": {"bsonType": "int"},           # SoT
                        "goals_per_shot": {"bsonType": "double"},         # G_by_Sh
                        "goals_per_shot_on_target": {"bsonType": "double"},# G_by_SoT
                        "average_shot_distance": {"bsonType": "double"},  # Dist
                        "free_kicks": {"bsonType": "int"},                # FK
                        "npxG_per_shot": {"bsonType": "double"},          # npxG_by_Sh
                        "goals_minus_xG": {"bsonType": "double"},         # G-xG
                        "np_goals_minus_xG": {"bsonType": "double"},      # np:G-xG
                    }
                },
                # Estadísticas de Pases
                "passing": {
                    "bsonType": "object",
                    "properties": {
                        "passes_completed": {"bsonType": "int"},          # Total_Cmp
                        "passes_attempted": {"bsonType": "int"},          # Total_Att
                        "pass_completion_pct": {"bsonType": "double"},    # Cmp
                        "total_pass_distance": {"bsonType": "int"},       # Total_TotDist
                        "progressive_pass_distance": {"bsonType": "int"}, # Total_PrgDist
                        "expected_assists": {"bsonType": "double"},       # xA
                        "assists_minus_xAG": {"bsonType": "double"},      # A-xAG
                        "key_passes": {"bsonType": "int"},                # KP
                        "passes_into_final_third": {"bsonType": "int"},   # LastThird
                        "passes_into_penalty_area": {"bsonType": "int"},  # PPA
                        "crosses_into_penalty_area": {"bsonType": "int"}, # CrsPA
                        "progressive_passes": {"bsonType": "int"},        # Prog
                    }
                },
                # Creación de Ocasiones
                "goal_shot_creation": {
                    "bsonType": "object",
                    "properties": {
                        "shot_creating_actions": {"bsonType": "int"},     # SCA
                        "sca_pass_live": {"bsonType": "int"},             # SCA_PassLive
                        "sca_pass_dead": {"bsonType": "int"},             # SCA_PassDead
                        "sca_dribble": {"bsonType": "int"},               # SCA_Drib
                        "sca_shot": {"bsonType": "int"},                  # SCA_Sh
                        "sca_foul_drawn": {"bsonType": "int"},            # SCA_Fld
                        "sca_defense": {"bsonType": "int"},               # SCA_Def
                        "goal_creating_actions": {"bsonType": "int"},     # GCA
                        "gca_pass_live": {"bsonType": "int"},             # GCA_PassLive
                        "gca_pass_dead": {"bsonType": "int"},             # GCA_PassDead
                        "gca_dribble": {"bsonType": "int"},               # GCA_Drib
                        "gca_shot": {"bsonType": "int"},                  # GCA_Sh
                        "gca_foul_drawn": {"bsonType": "int"},            # GCA_Fld
                        "gca_defense": {"bsonType": "int"},               # GCA_Def
                    }
                },
                # Estadísticas Defensivas
                "defense": {
                    "bsonType": "object",
                    "properties": {
                        "tackles": {"bsonType": "int"},                   # Tackles_Tkl
                        "tackles_won": {"bsonType": "int"},               # Tackles_TklW
                        "tackles_def_3rd": {"bsonType": "int"},           # Tackles_Def3rd
                        "tackles_mid_3rd": {"bsonType": "int"},           # Tackles_Mid3rd
                        "tackles_att_3rd": {"bsonType": "int"},           # Tackles_Att3rd
                        "dribble_tackles": {"bsonType": "int"},           # VsDribbles_Tkl
                        "dribbles_against": {"bsonType": "int"},          # VsDribbles_Att
                        "dribbled_past": {"bsonType": "int"},             # VsDribbles_Past
                        "blocks": {"bsonType": "int"},                    # Blocks_Blocks
                        "blocked_shots": {"bsonType": "int"},             # Blocks_Sh
                        "blocked_passes": {"bsonType": "int"},            # Blocks_Pass
                        "interceptions": {"bsonType": "int"},             # Int
                        "tackles_interceptions": {"bsonType": "int"},     # Tkl_plus_Int
                        "clearances": {"bsonType": "int"},                # Clr
                        "errors": {"bsonType": "int"},                    # Err
                    }
                },
            }
        }
    }

    try:
        db.create_collection("players", validator=player_schema)
        print("Colección 'players' creada con éxito.")
    except Exception as e:
        print(f"La colección 'players' ya existe o hubo un error.")

def insert_players_data(db):

    players_collection = db['players']
    df = pd.read_csv('all_players.csv')

    # Aquí me aseguro de que al menos 5 de las 100 jugadoras jueguen en un equipo que empiece por Manchester.
    manchester_players = df[df['Squad'].str.startswith('Manchester', na=False)]
    manchester_players = manchester_players.head(5)
    other_players = df[~df['Squad'].str.startswith('Manchester', na=False)].head(100 - len(manchester_players))

    # Combinar los dos DataFrames
    combined_df = pd.concat([manchester_players, other_players])

    records = []

    for index, row in combined_df.iterrows():
        nationality_value = row['Nation']
        if not pd.isna(nationality_value):
            nationality = nationality_value
        else:
            nationality = ''

        start_year = random.randint(2012, 2024)
        player = {
            # Información Personal
            'player_id': str(row['Player_id']),
            'name': row['Player'],
            'nationality': nationality,
            'position': row['Pos'],
            'age': int(str(row['Age']).split('-')[0]) if not pd.isna(row['Age']) else None,
            'birth_year': int(row['Born']) if not pd.isna(row['Born']) else None,
            'start_year': start_year,
            # Información del Equipo
            'team': {
                'squad_id': str(row['Squad_id']),
                'name': row['Squad']
            },
            # Estadísticas Básicas
            'stats': {
                'matches_played': int(row['MP']) if not pd.isna(row['MP']) else 0,
                'starts': int(row['Starts']) if not pd.isna(row['Starts']) else 0,
                'minutes_played': int(row['Min']) if not pd.isna(row['Min']) else 0,
                'goals': int(row['Gls']) if not pd.isna(row['Gls']) else 0,
                'assists': int(row['Ast']) if not pd.isna(row['Ast']) else 0,
                'goals_minus_pks': int(row['G-PK']) if not pd.isna(row['G-PK']) else 0,
                'penalty_goals': int(row['PK']) if not pd.isna(row['PK']) else 0,
                'penalty_attempts': int(row['PKatt']) if not pd.isna(row['PKatt']) else 0,
                'yellow_cards': int(row['CrdY']) if not pd.isna(row['CrdY']) else 0,
                'red_cards': int(row['CrdR']) if not pd.isna(row['CrdR']) else 0,
                'second_yellow_cards': int(row['2CrdY']) if '2CrdY' in row and not pd.isna(row['2CrdY']) else 0,
            },
            # Estadísticas Avanzadas
            'advanced_stats': {
                'xG': float(row['xG']) if not pd.isna(row['xG']) else 0.0,
                'npxG': float(row['npxG']) if not pd.isna(row['npxG']) else 0.0,
                'xAG': float(row['xAG']) if not pd.isna(row['xAG']) else 0.0,
                'shots': int(row['Sh']) if not pd.isna(row['Sh']) else 0,
                'shots_on_target': int(row['SoT']) if not pd.isna(row['SoT']) else 0,
                'goals_per_shot': float(row['G_by_Sh']) if not pd.isna(row['G_by_Sh']) else 0.0,
                'goals_per_shot_on_target': float(row['G_by_SoT']) if not pd.isna(row['G_by_SoT']) else 0.0,
                'average_shot_distance': float(row['Dist']) if not pd.isna(row['Dist']) else 0.0,
                'free_kicks': int(row['FK']) if not pd.isna(row['FK']) else 0,
                'npxG_per_shot': float(row['npxG_by_Sh']) if not pd.isna(row['npxG_by_Sh']) else 0.0,
                'goals_minus_xG': float(row['G-xG']) if not pd.isna(row['G-xG']) else 0.0,
                'np_goals_minus_xG': float(row['np:G-xG']) if not pd.isna(row['np:G-xG']) else 0.0,
            },
            # Estadísticas de Pases
            'passing': {
                'passes_completed': int(row['Total_Cmp']) if not pd.isna(row['Total_Cmp']) else 0,
                'passes_attempted': int(row['Total_Att']) if not pd.isna(row['Total_Att']) else 0,
                'pass_completion_pct': float(row['Cmp']) if not pd.isna(row['Cmp']) else 0.0,
                'total_pass_distance': int(row['Total_TotDist']) if not pd.isna(row['Total_TotDist']) else 0,
                'progressive_pass_distance': int(row['Total_PrgDist']) if not pd.isna(row['Total_PrgDist']) else 0,
                'expected_assists': float(row['xA']) if not pd.isna(row['xA']) else 0.0,
                'assists_minus_xAG': float(row['A-xAG']) if not pd.isna(row['A-xAG']) else 0.0,
                'key_passes': int(row['KP']) if not pd.isna(row['KP']) else 0,
                'passes_into_final_third': int(row['LastThird']) if not pd.isna(row['LastThird']) else 0,
                'passes_into_penalty_area': int(row['PPA']) if not pd.isna(row['PPA']) else 0,
                'crosses_into_penalty_area': int(row['CrsPA']) if not pd.isna(row['CrsPA']) else 0,
                'progressive_passes': int(row['Prog']) if not pd.isna(row['Prog']) else 0,
            },
            # Creación de Ocasiones
            'goal_shot_creation': {
                'shot_creating_actions': int(row['SCA']) if not pd.isna(row['SCA']) else 0,
                'sca_pass_live': int(row['SCA_PassLive']) if not pd.isna(row['SCA_PassLive']) else 0,
                'sca_pass_dead': int(row['SCA_PassDead']) if not pd.isna(row['SCA_PassDead']) else 0,
                'sca_dribble': int(row['SCA_Drib']) if not pd.isna(row['SCA_Drib']) else 0,
                'sca_shot': int(row['SCA_Sh']) if not pd.isna(row['SCA_Sh']) else 0,
                'sca_foul_drawn': int(row['SCA_Fld']) if not pd.isna(row['SCA_Fld']) else 0,
                'sca_defense': int(row['SCA_Def']) if not pd.isna(row['SCA_Def']) else 0,
                'goal_creating_actions': int(row['GCA']) if not pd.isna(row['GCA']) else 0,
                'gca_pass_live': int(row['GCA_PassLive']) if not pd.isna(row['GCA_PassLive']) else 0,
                'gca_pass_dead': int(row['GCA_PassDead']) if not pd.isna(row['GCA_PassDead']) else 0,
                'gca_dribble': int(row['GCA_Drib']) if not pd.isna(row['GCA_Drib']) else 0,
                'gca_shot': int(row['GCA_Sh']) if not pd.isna(row['GCA_Sh']) else 0,
                'gca_foul_drawn': int(row['GCA_Fld']) if not pd.isna(row['GCA_Fld']) else 0,
                'gca_defense': int(row['GCA_Def']) if not pd.isna(row['GCA_Def']) else 0,
            },
            # Estadísticas Defensivas
            'defense': {
                'tackles': int(row['Tackles_Tkl']) if not pd.isna(row['Tackles_Tkl']) else 0,
                'tackles_won': int(row['Tackles_TklW']) if not pd.isna(row['Tackles_TklW']) else 0,
                'tackles_def_3rd': int(row['Tackles_Def3rd']) if not pd.isna(row['Tackles_Def3rd']) else 0,
                'tackles_mid_3rd': int(row['Tackles_Mid3rd']) if not pd.isna(row['Tackles_Mid3rd']) else 0,
                'tackles_att_3rd': int(row['Tackles_Att3rd']) if not pd.isna(row['Tackles_Att3rd']) else 0,
                'dribble_tackles': int(row['VsDribbles_Tkl']) if not pd.isna(row['VsDribbles_Tkl']) else 0,
                'dribbles_against': int(row['VsDribbles_Att']) if not pd.isna(row['VsDribbles_Att']) else 0,
                'dribbled_past': int(row['VsDribbles_Past']) if not pd.isna(row['VsDribbles_Past']) else 0,
                'blocks': int(row['Blocks_Blocks']) if not pd.isna(row['Blocks_Blocks']) else 0,
                'blocked_shots': int(row['Blocks_Sh']) if not pd.isna(row['Blocks_Sh']) else 0,
                'blocked_passes': int(row['Blocks_Pass']) if not pd.isna(row['Blocks_Pass']) else 0,
                'interceptions': int(row['Int']) if not pd.isna(row['Int']) else 0,
                'tackles_interceptions': int(row['Tkl_plus_Int']) if not pd.isna(row['Tkl_plus_Int']) else 0,
                'clearances': int(row['Clr']) if not pd.isna(row['Clr']) else 0,
                'errors': int(row['Err']) if not pd.isna(row['Err']) else 0,
            },
            
        }

        records.append(player)

    if records:
        players_collection.insert_many(records)
        print(f"{len(records)} jugadores insertados correctamente.")
    else:
        print("No se encontraron registros para insertar.")


def update_player_names_to_uppercase(db):
    players_collection = db['players']
    players = list(players_collection.find().limit(2))
    for player in players:
        updated_name = player['name'].upper()
        players_collection.update_one(
            {'_id': player['_id']},
            {'$set': {'name': updated_name}}
        )
        print(f"Updated {player['name']} to {updated_name}")

def find_players_started_after_2020(db):
    players_collection = db['players']
    result = players_collection.find({'start_year': {'$gt': 2020}})
    for player in result:
        print(player)

def find_players_by_team_name(db):
    players_collection = db['players']
    result = players_collection.find({'team.name': {'$regex': '^Manchester'}})
    for player in result:
        print(player)

def find_players_by_nationality(db, country):
    players_collection = db['players']
    result = players_collection.find({'nationality': country})
    for player in result:
        print(player)


def measure_query_time(db):
    players_collection = db['players']

    start_time = time.time()
    result = list(players_collection.find({'start_year': {'$gt': 2020}}))
    end_time = time.time()
    print(f"Consulta 1: Tiempo de ejecución: {end_time - start_time:.4f} segundos. Resultados: {len(result)}")

    start_time = time.time()
    result = list(players_collection.find({'team.name': {'$regex': '^Manchester', '$options': 'i'}}))
    end_time = time.time()
    print(f"Consulta 2: Tiempo de ejecución: {end_time - start_time:.4f} segundos. Resultados: {len(result)}")

    start_time = time.time()
    result = list(players_collection.find({'nationality': 'es ESP'}))
    end_time = time.time()
    print(f"Consulta 3: Tiempo de ejecución: {end_time - start_time:.4f} segundos. Resultados: {len(result)}")



if __name__ == "__main__":
    client = MongoClient('mongodb://root:root@localhost:27017/')
    db = client['women-football']
    if 'players' in db.list_collection_names():
        db['players'].drop()
    create_players_collection(db=db)
    insert_players_data(db=db)
    measure_query_time(db=db)
    update_player_names_to_uppercase(db=db)

    while True:

        print("\n=== MENÚ DE CONSULTAS ===")
        print("1. Salir")
        print("2. Filtrar por start_year mayor a 2020")
        print("3. Filtrar por equipos que empiecen por 'Manchester'")
        print("4. Filtrar por nacionalidad")

        choice = input("Elige una opción (1-4): ")

        if choice == "1":
            print("Saliendo del programa...")
            break
        elif choice == "2":
            print("\nFILTRAR POR start_year MAYOR A 2020")
            find_players_started_after_2020(db=db)
        elif choice == "3":
            print("\nFILTRAR POR EQUIPOS QUE EMPIECEN POR 'Manchester'")
            find_players_by_team_name(db=db)
        elif choice == "4":
            print(f"\nFILTRAR POR NACIONALIDAD es ESP")
            find_players_by_nationality(db, 'es ESP')            
        else:
            print("Opción no válida. Por favor, elige una opción del 1 al 4.")
