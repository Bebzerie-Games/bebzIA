import discord
import os
from dotenv import load_dotenv

# Charger les variables d'environnement du fichier .env
load_dotenv()
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")

# Définir les intents (permissions) du bot
intents = discord.Intents.default()
intents.messages = True  # Pour recevoir des événements de message
intents.message_content = True # Pour lire le contenu des messages (nécessaire pour les commandes et l'analyse)
# intents.guilds = True # Inclus par défaut, mais bon à savoir

client = discord.Client(intents=intents)

@client.event
async def on_ready():
    print(f'Bot connecté en tant que {client.user.name}')
    print(f'ID du bot : {client.user.id}')
    print('------')

@client.event
async def on_message(message):
    # Ne pas répondre aux messages du bot lui-même
    if message.author == client.user:
        return

    # Une commande de test simple
    if message.content.startswith('!ping'):
        await message.channel.send('Pong!')

    # Répondre si le bot est mentionné
    if client.user.mentioned_in(message):
        await message.channel.send(f'Salut {message.author.mention} ! Je suis là.')

if __name__ == "__main__":
    if DISCORD_BOT_TOKEN is None:
        print("Erreur : Le token du bot Discord n'est pas défini. Vérifiez votre fichier .env ou vos variables d'environnement.")
    else:
        try:
            client.run(DISCORD_BOT_TOKEN)
        except discord.errors.LoginFailure:
            print("Erreur : Échec de la connexion. Le token du bot Discord est incorrect.")
        except Exception as e:
            print(f"Une erreur est survenue lors du lancement du bot : {e}")