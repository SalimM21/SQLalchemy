from sqlalchemy import create_engine, MetaData, Table, insert, select
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import urllib.parse
import os

# Charger les variables d'environnement
load_dotenv()

# Connexion à la base
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = urllib.parse.quote_plus(os.getenv("DB_PASSWORD"))
DB_NAME = os.getenv("DB_NAME")
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)
metadata = MetaData()
clients = Table("clients", metadata, autoload_with=engine)
destinations = Table("destinations", metadata, autoload_with=engine)

clients_data = [
    {"first_name": "Omar", "last_name": "Hitar", "email": "omar@example.com", "phone_number": "0612345678"},
    {"first_name": "Lina", "last_name": "Karim", "email": "lina@example.com", "phone_number": "0611111111"},
    {"first_name": "Sami", "last_name": "Benali", "email": "sami@example.com", "phone_number": "0622222222"},
    {"first_name": "Nora", "last_name": "Hassan", "email": "nora@example.com", "phone_number": "0633333333"},
    {"first_name": "Youssef", "last_name": "Tariq", "email": "youssef@example.com", "phone_number": "0644444444"},
]

try:
    with engine.begin() as conn:
        conn.execute(insert(clients), clients_data)
        print(" Clients insérés avec succès !")
except SQLAlchemyError as e:
    print(f" Erreur lors de l'insertion : {e}")
# Insertion des destinations (au moins 5)
try:
    with engine.begin() as conn:
        conn.execute(insert(destinations), [
            {'name': 'Paris Tour', 'country': 'France', 'price_per_person': 250.0},
            {'name': 'Sahara Adventure', 'country': 'Morocco', 'price_per_person': 300.0},
            {'name': 'Tokyo Discovery', 'country': 'Japan', 'price_per_person': 500.0},
            {'name': 'New York Escape', 'country': 'USA', 'price_per_person': 450.0},
            {'name': 'Barcelona Beaches', 'country': 'Spain', 'price_per_person': 280.0},
            {'name': 'Petra Experience', 'country': 'Jordan', 'price_per_person': 320.0}
        ])
        print(" Destinations insérées avec succès !")
except SQLAlchemyError as e:
    print(f" Erreur lors de l'insertion des destinations : {e}")

# Récupération de 3 clients (assure-toi qu’ils existent déjà)
client_ids = conn.execute(select(clients.c.client_id)).fetchmany(3)

# Insertion de 3 réservations (bookings) pour des clients différents
booking_result = conn.execute(insert(bookings).returning(bookings.c.booking_id), [
    {
        'client_id': client_ids[0][0],
        'booking_date': datetime(2025, 7, 5),
        'total_price': 600.0
    },
    {
        'client_id': client_ids[1][0],
        'booking_date': datetime(2025, 7, 6),
        'total_price': 750.0
    },
    {
        'client_id': client_ids[2][0],
        'booking_date': datetime(2025, 7, 7),
        'total_price': 920.0
    }
])

# Optionnel : récupérer les booking_id insérés pour d'autres insertions (ex: booking_items)
booking_ids = booking_result.fetchall()
print(" Réservations insérées :", booking_ids)

# Sélection de tous les clients avec nom, email et téléphone
result = conn.execute(
    select(clients.c.first_name, clients.c.email, clients.c.phone_number)
)

print(" Liste des clients inscrits :")
for row in result:
    print(f" Nom : {row.first_name}, Email : {row.email}, Téléphone : {row.phone_number}")