from sqlalchemy import create_engine, Table, Column, Integer, String, Float, DateTime, ForeignKey, MetaData
from dotenv import load_dotenv
from sqlalchemy import insert
from datetime import datetime
import urllib.parse
import os

# Charger les variables d'environnement
load_dotenv()

# Récupérer les informations de connexion
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = urllib.parse.quote_plus(os.getenv("DB_PASSWORD"))
DB_NAME = os.getenv("DB_NAME")

# Créer la chaîne de connexion et l'engine
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# Créer un objet MetaData
metadata = MetaData()

# Créer la table CLients
clients = Table(
    "clients",
    metadata,
    Column("client_id", Integer, primary_key=True, autoincrement=True),
    Column("first_name", String(50), nullable=False),
    Column("last_name", String(50), nullable=False),
    Column("email", String(100), nullable=False, unique=True),
    Column("phone_number", String(20), nullable=True),
)

# Créer la table destinations : destination_id (PK), name, country, price_per_person.
destinations_table = Table(
    "destinations",
    metadata,
    Column("destination_id", Integer, primary_key=True),
    Column("name", String(100), nullable=False),
    Column("country", String(100), nullable=False),
    Column("price_per_person", Float, nullable=False),
)

# Créer la table bookings : booking_id (PK), client_id (FK), booking_date, total_price.
bookings_table = Table(
    "bookings",
    metadata,
    Column("booking_id", Integer, primary_key=True),
    Column("client_id", Integer, ForeignKey("clients.client_id"), nullable=False),
    Column("booking_date", DateTime, nullable=False),
    Column("total_price", Float, nullable=False),
)

# Créer la table booking_items : item_id (PK), booking_id (FK), destination_id (FK), travelers_count
booking_items_table = Table(
    "booking_items",
    metadata,
    Column("item_id", Integer, primary_key=True),
    Column("booking_id", Integer, ForeignKey("bookings.booking_id"), nullable=False),
    Column("destination_id", Integer, ForeignKey("destinations.destination_id"), nullable=False),
    Column("travelers_count", Integer, nullable=False),
)

if __name__ == "__main__":
    metadata.create_all(engine)
    print(" Table 'clients' créée avec succès.")
    print(" Table 'destinations' créée avec succès.")
    print(" Table 'bookings' créée avec succès.")
    print(" Table 'booking_items' créée avec succès.")

# Challenge 3 : Insertion de données d’exemple
# Insérer des données fictives dans les tables pour tests et démonstrations.

# Connexion à la base de données
with engine.connect() as conn:
    # Ajouter des clients
    clients_data = [
        {"first_name": "Alice", "last_name": "Dupont", "email": "Alice@gmail.com", "phone_number": "0123456789"},
        {"first_name": "Bob", "last_name": "Martin", "email": "Bob@gmail.com", "phone_number": "0987654321"},
        {"first_name": "Charlie", "last_name": "Durand", "email": "Charlie@gmail.com", "phone_number": "0147258369"},
        {"first_name": "David", "last_name": "Lefebvre", "email": "David@gmail.com", "phone_number": "0162837459"},
        {"first_name": "Eva", "last_name": "Moreau", "email": "Eva@gmail.com", "phone_number": "0192837465"}
    ]
    conn.execute(insert(clients), clients_data)
    print(" Clients insérés avec succès.") 

# inserer des destinations
destinations_data = [
    {"name": "Paris", "country": "France", "price_per_person": 200.0},
    {"name": "New York", "country": "USA", "price_per_person": 300.0},
    {"name": "Tokyo", "country": "Japan", "price_per_person": 400.0},
    {"name": "Sydney", "country": "Australia", "price_per_person": 500.0},
    {"name": "Cape Town", "country": "South Africa", "price_per_person": 250.0}
]
with engine.connect() as conn:
    conn.execute(insert(destinations_table), destinations_data)
    print(" Destinations insérées avec succès.")   

insert_bookings = insert(bookings_table).values(
    [
        {"client_id": 1, "booking_date": datetime.now(), "total_price": 600.0},
        {"client_id": 2, "booking_date": datetime.now(), "total_price": 800.0},
        {"client_id": 3, "booking_date": datetime.now(), "total_price": 1000.0}
    ]
)

with engine.connect() as conn:
    conn.execute(insert_bookings)
    print(" Réservations insérées avec succès.")

# Ajouter des lignes dans booking_items pour lier les destinations aux réservations avec un nombre de voyageurs.
booking_items_data = [
    {"booking_id": 1, "destination_id": 1, "travelers_count": 2},
    {"booking_id": 1, "destination_id": 2, "travelers_count": 1},
    {"booking_id": 2, "destination_id": 3, "travelers_count": 3},
    {"booking_id": 3, "destination_id": 4, "travelers_count": 4},
    {"booking_id": 3, "destination_id": 5, "travelers_count": 2}
]

with engine.connect() as conn:
    conn.execute(insert(booking_items_table), booking_items_data)
    print(" Booking items insérés avec succès.")

# Afficher tous les clients (first_name, email, phone_number)
with engine.connect() as conn:
    result = conn.execute("SELECT first_name, email, phone_number FROM clients")
    for row in result:
        print(row)