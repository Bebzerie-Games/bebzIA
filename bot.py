import discord
import os
from dotenv import load_dotenv
from azure.cosmos import CosmosClient, PartitionKey, exceptions

# Charger les variables d'environnement du fichier .env
load_dotenv()
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
COSMOS_DB_ENDPOINT = os.getenv("COSMOS_DB_ENDPOINT")
COSMOS_DB_KEY = os.getenv("COSMOS_DB_KEY")
DATABASE_NAME = os.getenv("DATABASE_NAME")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")

# Initialisation du client Cosmos DB
cosmos_client = None
database_client = None
container_client = None

if COSMOS_DB_ENDPOINT and COSMOS_DB_KEY and DATABASE_NAME and CONTAINER_NAME:
    try:
        cosmos_client = CosmosClient(COSMOS_DB_ENDPOINT, credential=COSMOS_DB_KEY)
        database_client = cosmos_client.get_database_client(DATABASE_NAME)
        # Nous allons créer la base de données et le conteneur si ils n'existent pas
        # dans le script fetch_new_messages.py, ici on essaie juste de les récupérer
        # pour vérifier la connexion initiale.
        # Pour l'instant, nous ne créons pas la DB ou le conteneur ici pour garder bot.py simple.
        # La création sera gérée par fetch_new_messages.py.
        container_client = database_client.get_container_client(CONTAINER_NAME)
        print("Connexion à Cosmos DB initialisée.")
    except Exception as e:
        print(f"Erreur lors de l'initialisation de la connexion Cosmos DB: {e}")
else:
    print("Avertissement : Informations de connexion Cosmos DB manquantes. Le client Cosmos DB ne sera pas initialisé.")


# Définir les intents (permissions) du bot
intents = discord.Intents.default()
intents.messages = True
intents.message_content = True

client = discord.Client(intents=intents)

@client.event
async def on_ready():
    print(f'Bot connecté en tant que {client.user.name}')
    print(f'ID du bot : {client.user.id}')
    if container_client:
        try:
            # Juste pour vérifier que le conteneur est accessible, sans faire de vraie requête coûteuse
            container_properties = container_client.read()
            print(f"Connecté au conteneur Cosmos DB: {container_properties['id']}")
        except exceptions.CosmosResourceNotFoundError:
            print(f"Avertissement : Le conteneur '{CONTAINER_NAME}' n'existe pas encore dans la base de données '{DATABASE_NAME}'. Il sera créé par le script de récupération.")
        except Exception as e:
            print(f"Erreur lors de la vérification du conteneur Cosmos DB : {e}")
    print('------')

@client.event
async def on_message(message):
    if message.author == client.user:
        return

    if message.content.startswith('!ping'):
        await message.channel.send('Pong!')

    if client.user.mentioned_in(message):
        await message.channel.send(f'Salut {message.author.mention} ! Je suis là.')

if __name__ == "__main__":
    if DISCORD_BOT_TOKEN is None:
        print("Erreur : Le token du bot Discord n'est pas défini.")
    else:
        try:
            client.run(DISCORD_BOT_TOKEN)
        except discord.errors.LoginFailure:
            print("Erreur : Échec de la connexion. Le token du bot Discord est incorrect.")
        except Exception as e:
            print(f"Une erreur est survenue lors du lancement du bot : {e}")
