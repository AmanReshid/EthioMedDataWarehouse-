from telethon import TelegramClient
import csv
import os
import logging
from dotenv import load_dotenv
import asyncio

# Load environment variables
load_dotenv('.env')
api_id = os.getenv('TG_API_ID')
api_hash = os.getenv('TG_API_HASH')
phone = os.getenv('phone')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize the client once
client = TelegramClient('scraping_session', api_id, api_hash)

# Function to scrape data from a single channel
async def scrape_channel(client, channel_username, writer, media_dir):
    try:
        entity = await client.get_entity(channel_username)
        channel_title = entity.title  # Extract the channel's title
        async for message in client.iter_messages(entity, limit=10000):
            media_path = None
            if message.media and hasattr(message.media, 'photo'):
                # Create a unique filename for the photo
                filename = f"{channel_username}_{message.id}.jpg"
                media_path = os.path.join(media_dir, filename)
                # Download the media to the specified directory if it's a photo
                await client.download_media(message.media, media_path)
            
            # Write the channel title along with other data
            writer.writerow([channel_title, channel_username, message.id, message.message, message.date, media_path])

            logging.info(f"Downloaded message {message.id} from {channel_username}")

    except Exception as e:
        logging.error(f"Error scraping channel {channel_username}: {e}")

async def main():
    try:
        await client.start()  # Start the client
        logging.info('Telegram client started successfully.')
        
        # Create a directory for media files
        media_dir = 'photos'
        os.makedirs(media_dir, exist_ok=True)

        # Prepare the data directory and open the CSV file
        data_dir = os.path.join('..', 'data')
        os.makedirs(data_dir, exist_ok=True)  # Create the data directory if it doesn't exist
        
        csv_file_path = os.path.join(data_dir, 'telegram_data.csv')
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Channel Title', 'Channel Username', 'ID', 'Message', 'Date', 'Media Path'])  # Include channel title in the header
            
            # List of channels to scrape
            channels = [
                '@DoctorsET',
                '@Chemed Telegram Channel',
                '@lobelia4cosmetics',
                '@yetenaweg',
                '@EAHCI'
            ]
            
            # Iterate over channels and scrape data into the single CSV file
            for channel in channels:
                await scrape_channel(client, channel, writer, media_dir)
                logging.info(f"Scraped data from {channel}")

    except Exception as e:
        logging.error(f'Error occurred in the main process: {e}')
    
    finally:
        await client.disconnect()  # Disconnect the client after finishing
        logging.info('Disconnected from Telegram client.')

