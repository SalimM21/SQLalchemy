import os
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, ForeignKey, insert, select
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import join, text, update, func
from sqlalchemy.sql import select

# Charger les variables d'environnement
load_dotenv()

# Configuration de la base de données
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = "SQLalchemy"
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL, echo=True)

metadata = MetaData()

# Définition des tables
clients = Table(
    'clients', metadata,
    Column('client_id', Integer, primary_key=True),
    Column('first_name', String(50), nullable=False),
    Column('last_name', String(50), nullable=False),
    Column('email', String(100), nullable=False),
    Column('phone_number', String(20))
)

destinations = Table(
    'destinations', metadata,
    Column('destination_id', Integer, primary_key=True),
    Column('name', String(100), nullable=False),
    Column('country', String(50), nullable=False),
    Column('price_per_person', Float, nullable=False)
)

bookings = Table(
    'bookings', metadata,
    Column('booking_id', Integer, primary_key=True),
    Column('client_id', Integer, ForeignKey('clients.client_id'), nullable=False),
    Column('booking_date', DateTime, default=datetime.utcnow),
    Column('total_price', Float, nullable=False)
)

booking_items = Table(
    'booking_items', metadata,
    Column('item_id', Integer, primary_key=True),
    Column('booking_id', Integer, ForeignKey('bookings.booking_id'), nullable=False),
    Column('destination_id', Integer, ForeignKey('destinations.destination_id'), nullable=False),
    Column('travelers_count', Integer, nullable=False)
)

# Créer les tables si elles n'existent pas
metadata.create_all(engine)

try:
    with engine.begin() as connection:
        # Vérification de la connexion
        result = connection.execute(select([text("version()")]))
        print("✅ Connexion réussie à PostgreSQL :", result.fetchone())

        # Insertion des clients
        connection.execute(insert(clients), [
            {'first_name': 'Alice', 'last_name': 'Durand', 'email': 'alice.durand@example.com', 'phone_number': '0612345678'},
            {'first_name': 'Bob', 'last_name': 'Martin', 'email': 'bob.martin@example.com', 'phone_number': '0623456789'},
            {'first_name': 'Carla', 'last_name': 'Lopez', 'email': 'carla.lopez@example.com', 'phone_number': '0634567890'},
            {'first_name': 'David', 'last_name': 'Nguyen', 'email': 'david.nguyen@example.com', 'phone_number': '0645678901'},
            {'first_name': 'Emma', 'last_name': 'Kassimi', 'email': 'emma.kassimi@example.com', 'phone_number': '0656789012'}
        ])

        # Insertion des destinations
        connection.execute(insert(destinations), [
            {'name': 'Paris Tour', 'country': 'France', 'price_per_person': 250.0},
            {'name': 'Sahara Adventure', 'country': 'Morocco', 'price_per_person': 300.0},
            {'name': 'Tokyo Discovery', 'country': 'Japan', 'price_per_person': 500.0}
        ])

        # Récupération des IDs clients et destinations
        client_ids = connection.execute(select(clients.c.client_id)).fetchmany(3)
        destination_ids = connection.execute(select(destinations.c.destination_id)).fetchmany(3)

        # Insertion des bookings
        booking_result = connection.execute(insert(bookings).returning(bookings.c.booking_id), [
            {'client_id': client_ids[0][0], 'booking_date': datetime(2025, 7, 1), 'total_price': 500.0},
            {'client_id': client_ids[1][0], 'booking_date': datetime(2025, 7, 2), 'total_price': 900.0},
            {'client_id': client_ids[2][0], 'booking_date': datetime(2025, 7, 3), 'total_price': 1500.0}
        ])
        booking_ids = booking_result.fetchall()

        # Insertion des éléments de réservation
        connection.execute(insert(booking_items), [
            {'booking_id': booking_ids[0][0], 'destination_id': destination_ids[0][0], 'travelers_count': 2},
            {'booking_id': booking_ids[1][0], 'destination_id': destination_ids[1][0], 'travelers_count': 3},
            {'booking_id': booking_ids[2][0], 'destination_id': destination_ids[2][0], 'travelers_count': 4}
        ])
        print(" Données insérées avec succès dans les tables.")
except SQLAlchemyError as e:
    print(" Erreur lors de l'insertion des données :", e)

# Afficher les données insérées pour vérification
with engine.connect() as connection:
    print("Clients :")
    for row in connection.execute(select(clients)):
        print(row)

    print("\nDestinations :")
    for row in connection.execute(select(destinations)):
        print(row)

    print("\nBookings :")
    for row in connection.execute(select(bookings)):
        print(row)

    print("\nBooking Items :")
    for row in connection.execute(select(booking_items)):
        print(row)

# Sélection des destinations avec un prix > 1000 €
result = connection.execute(
    select(destinations.c.name, destinations.c.country, destinations.c.price_per_person)
    .where(destinations.c.price_per_person > 1000)
)

print(" Destinations à plus de 1000 € par personne :")
for row in result:
    print(f" {row.name} ({row.country}) - {row.price_per_person:.2f} €")

# Challenge 6 : Requête avec jointure

# Afficher toutes les réservations avec
# Le nom du client
# La destination choisie
# Le nombre de voyageurs
# Le prix par personne

# Jointure entre clients, bookings, booking_items, destinations
stmt = (
    select(
        clients.c.first_name.label("client_name"),
        destinations.c.name.label("destination_name"),
        booking_items.c.travelers_count,
        destinations.c.price_per_person
    )
    .select_from(
        clients
        .join(bookings, clients.c.client_id == bookings.c.client_id)
        .join(booking_items, bookings.c.booking_id == booking_items.c.booking_id)
        .join(destinations, booking_items.c.destination_id == destinations.c.destination_id)
    )
)

result = connection.execute(stmt)

# Affichage
print(" Liste des réservations détaillées :")
for row in result:
    print(
        f" Client : {row.client_name}, "
        f" Destination : {row.destination_name}, "
        f" Voyageurs : {row.travelers_count}, "
        f" Prix/pers : {row.price_per_person:.2f} €"
    )

# Mettre à jour l’adresse e-mail d’un client avec un client_id donné
# Exemple : mettre à jour l’email du client n°2
stmt = (
    update(clients)
    .where(clients.c.client_id == 2)
    .values(email="hakim.email@example.com")
)

result = connection.execute(stmt)
print(f" {result.rowcount} ligne(s) mise(s) à jour.")

# Réduire de 10 % le prix de toutes les destinations situées dans un pays donné
country_to_discount = 'Morocco'

stmt = (
    update(destinations)
    .where(destinations.c.country == country_to_discount)
    .values(price_per_person=destinations.c.price_per_person * 0.9)
)

result = connection.execute(stmt)
print(f" {result.rowcount} destination(s) mises à jour avec une réduction de 10 % pour le pays '{country_to_discount}'.")

# Compter le nombre total de clients
stmt = select([text("COUNT(*)")]).select_from(clients)
result = connection.execute(stmt)
total_clients = result.scalar()
print(f" Nombre total de clients : {total_clients}")

# Afficher la moyenne de prix des destinations.
# Calcul de la moyenne du prix par personne
result = connection.execute(
    select(func.avg(destinations.c.price_per_person))
)

average_price = result.scalar_one()
print(f" Prix moyen des destinations : {average_price:.2f} €")

# Calculer le nombre total de voyageurs (somme de travelers_count) par destination

stmt = (
    select(
        destinations.c.name,
        func.sum(booking_items.c.travelers_count).label("total_travelers")
    )
    .join(booking_items, destinations.c.destination_id == booking_items.c.destination_id)
    .group_by(destinations.c.name)
)

result = connection.execute(stmt)

print(" Nombre total de voyageurs par destination :")
for row in result:
    print(f" {row.name} : {row.total_travelers} voyageurs")

# Lister les destinations qui ont été réservées plus de 2 fois.
stmt = (
    select(
        destinations.c.name,
        func.count(booking_items.c.item_id).label("reservations_count")
    )
    .join(booking_items, destinations.c.destination_id == booking_items.c.destination_id)
    .group_by(destinations.c.name)
    .having(func.count(booking_items.c.item_id) > 2)
)

result = connection.execute(stmt)

print(" Destinations réservées plus de 2 fois :")
for row in result:
    print(f" {row.name} - Nombre de réservations : {row.reservations_count}")

# Afficher les clients dont le total de voyageurs dans leurs réservations est supérieur à 5
stmt = (
    select(
        clients.c.client_id,
        clients.c.first_name,
        clients.c.last_name,
        func.sum(booking_items.c.travelers_count).label("total_travelers")
    )
    .join(bookings, clients.c.client_id == bookings.c.client_id)
    .join(booking_items, bookings.c.booking_id == booking_items.c.booking_id)
    .group_by(clients.c.client_id, clients.c.first_name, clients.c.last_name)
    .having(func.sum(booking_items.c.travelers_count) > 5)
)

result = connection.execute(stmt)

print(" Clients avec plus de 5 voyageurs au total dans leurs réservations :")
for row in result:
    print(f" {row.first_name} {row.last_name} - Total voyageurs : {row.total_travelers}")
    