print("[FETCH_SCRIPT_DEBUG] Le fichier fetch_new_messages.py est en cours d'exécution - VERSION TEST (Agir après READY)")

import discord
import os
from dotenv import load_dotenv
import asyncio
import traceback
import logging

# Charger les variables d'environnement
load_dotenv()
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
LOG_CHANNEL_ID_STR = os.getenv("LOG_CHANNEL_ID")

# --- Configuration Globale du Client Discord ---
intents_log = discord.Intents.all() # Gardons Intents.all() pour l'instant
print(f"[DEBUG] Intents pour discord_log_client: Value={intents_log.value}")
discord_log_client = discord.Client(intents=intents_log)

async def run_script_minimal_connect_test():
    print("[DEBUG] run_script_minimal_connect_test: Début du test.")
    if not DISCORD_BOT_TOKEN:
        print("[DEBUG] DISCORD_BOT_TOKEN MANQUANT.")
        return
    
    log_channel_id_int = None
    if LOG_CHANNEL_ID_STR:
        try:
            log_channel_id_int = int(LOG_CHANNEL_ID_STR)
        except ValueError:
            print(f"[DEBUG] LOG_CHANNEL_ID_STR ('{LOG_CHANNEL_ID_STR}') n'est pas un entier.")
    else:
        print("[DEBUG] LOG_CHANNEL_ID_STR MANQUANT.")

    action_performed = False
    try:
        # Étape 1: Login (valide le token et récupère des infos HTTP)
        print("[DEBUG] Étape 1: Tentative de discord_log_client.login()...")
        await asyncio.wait_for(discord_log_client.login(DISCORD_BOT_TOKEN), timeout=20.0) # Timeout pour login
        print("[DEBUG] discord_log_client.login() terminé.")

        # Étape 2: Connect (connexion à la passerelle WebSocket)
        # connect() est une boucle. Nous la laissons tourner pendant un temps défini.
        # L'événement READY devrait être dispatché pendant ce temps.
        print("[DEBUG] Étape 2: Tentative de discord_log_client.connect()... (timeout 30s). Attente de l'événement READY pendant cette période.")
        try:
            # reconnect=False est bon pour un script qui s'exécute une fois.
            await asyncio.wait_for(discord_log_client.connect(reconnect=False), timeout=30.0)
            # Si connect() retourne avant le timeout, c'est généralement dû à une déconnexion ou une erreur interne.
            print("[DEBUG] discord_log_client.connect() s'est terminé avant le timeout (peut indiquer une déconnexion prématurée).")
        except asyncio.TimeoutError:
            # CE TIMEOUT EST ATTENDU si le client s'est connecté et est resté actif.
            print("[DEBUG] discord_log_client.connect() a atteint le timeout de 30s (signifie qu'il tournait).")
            # C'est ici que nous vérifions si le client est devenu prêt pendant ces 30s.
            if discord_log_client.is_ready():
                print(f"[DEBUG] Client EST PRÊT (connecté en tant que {discord_log_client.user}) après que connect() ait tourné.")
                
                # Étape 3: Action si prêt
                if log_channel_id_int:
                    log_channel_obj = discord_log_client.get_channel(log_channel_id_int)
                    if log_channel_obj:
                        print(f"[DEBUG] Tentative d'envoi de message test au canal {log_channel_id_int}")
                        await log_channel_obj.send(f"Message de test automatique (script Heroku run) - Client prêt @ {discord.utils.utcnow().isoformat()}Z")
                        print("[DEBUG] Message de test envoyé.")
                        action_performed = True
                    else:
                        print(f"[DEBUG] Canal de log {log_channel_id_int} non trouvé.")
                else:
                    print("[DEBUG] Pas d'ID de canal de log pour message test.")
            else:
                print("[DEBUG] Client N'EST PAS PRÊT même après que connect() ait tourné pendant 30s. L'événement READY n'a pas été traité correctement ou la connexion a échoué discrètement.")
        
        if not action_performed and not discord_log_client.is_ready():
             print("[DEBUG] Aucune action effectuée car le client n'est pas devenu prêt ou n'a pas pu envoyer de message.")


    except discord.errors.LoginFailure as lf:
        print(f"[DEBUG] LoginFailure - {lf}")
    except discord.errors.PrivilegedIntentsRequired as pir: # Au cas où
        print(f"[DEBUG] ERREUR PrivilegedIntentsRequired - {pir}.")
    except asyncio.TimeoutError: 
        # Ce bloc attraperait un timeout de login() s'il n'était pas géré plus spécifiquement
        print("[DEBUG] TIMEOUT global d'une opération (probablement login).")
    except Exception as e:
        error_details = traceback.format_exc()
        print(f"[DEBUG] ERREUR INATTENDUE - {e}\nTrace:\n{error_details}")
    finally:
        print("[DEBUG] Bloc Finally atteint.")
        # Correction de l'AttributeError: utiliser is_closed()
        if not discord_log_client.is_closed():
            print("[DEBUG] Tentative de déconnexion (client.close) dans finally...")
            await discord_log_client.close()
            print("[DEBUG] discord_log_client.close() appelé dans finally.")
        else:
            print("[DEBUG] Client déjà fermé (selon is_closed()) ou jamais connecté/ouvert proprement.")
        print("[DEBUG] Test de connexion minimal terminé (run_script_minimal_connect_test).")

if __name__ == "__main__":
    log_format = '%(asctime)s %(levelname)-8s %(name)-22s %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_format) # INFO pour moins de verbosité, DEBUG si besoin
    # Pour les logs de discord.py spécifiquement en DEBUG :
    discord_logger = logging.getLogger('discord')
    discord_logger.setLevel(logging.DEBUG) # Garder discord.py en DEBUG pour voir les événements
    # S'assurer qu'un handler est là si basicConfig n'en a pas mis un qui couvre ce logger
    if not discord_logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(log_format))
        discord_logger.addHandler(console_handler)
    
    print("[FETCH_SCRIPT_DEBUG] Entrée dans if __name__ == '__main__'.")
    print("Démarrage de run_script_minimal_connect_test...")
    try:
        print("RAPPEL: Assurez-vous que le dyno worker (bot.py) est arrêté.")
        asyncio.run(run_script_minimal_connect_test())
    except Exception as e:
        print(f"ERREUR FATALE au niveau de asyncio.run: {e}")
        traceback.print_exc()
    finally:
        print("[FETCH_SCRIPT_DEBUG] Exécution de __main__ terminée.")
