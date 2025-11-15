# This file defines all the Python classes that map to the sakila

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Numeric, Text, Boolean
from sqlalchemy.orm import relationship
from database import SakilaBase # Import the *Sakila* base class


class Country(SakilaBase):
    __tablename__ = 'country'
    country_id = Column(Integer, primary_key=True)
    country = Column(String)
    last_update = Column(DateTime)
    cities = relationship("City", back_populates="country")

class City(SakilaBase):
    __tablename__ = 'city'
    city_id = Column(Integer, primary_key=True)
    city = Column(String)
    country_id = Column(Integer, ForeignKey('country.country_id'))
    last_update = Column(DateTime)
    country = relationship("Country", back_populates="cities")
    addresses = relationship("Address", back_populates="city")

class Address(SakilaBase):
    __tablename__ = 'address'
    address_id = Column(Integer, primary_key=True)
    address = Column(String)
    city_id = Column(Integer, ForeignKey('city.city_id'))
    last_update = Column(DateTime)
    city = relationship("City", back_populates="addresses")
    customers = relationship("Customer", back_populates="address")
    stores = relationship("Store", back_populates="address")
    staff = relationship("Staff", back_populates="address")

class Customer(SakilaBase):
    __tablename__ = 'customer'
    customer_id = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey('store.store_id'))
    first_name = Column(String)
    last_name = Column(String)
    active = Column(Boolean)
    address_id = Column(Integer, ForeignKey('address.address_id'))
    last_update = Column(DateTime)
    address = relationship("Address", back_populates="customers")
    rentals = relationship("Rental", back_populates="customer")
    payments = relationship("Payment", back_populates="customer")

class Store(SakilaBase):
    __tablename__ = 'store'
    store_id = Column(Integer, primary_key=True)
    manager_staff_id = Column(Integer, ForeignKey('staff.staff_id'))
    address_id = Column(Integer, ForeignKey('address.address_id'))
    last_update = Column(DateTime)
    address = relationship("Address", back_populates="stores")
    inventories = relationship("Inventory", back_populates="store")

class Staff(SakilaBase):
    __tablename__ = 'staff'
    staff_id = Column(Integer, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    address_id = Column(Integer, ForeignKey('address.address_id'))
    store_id = Column(Integer, ForeignKey('store.store_id'))
    last_update = Column(DateTime)
    address = relationship("Address", back_populates="staff")
    rentals = relationship("Rental", back_populates="staff")
    payments = relationship("Payment", back_populates="staff")

class Language(SakilaBase):
    __tablename__ = 'language'
    language_id = Column(Integer, primary_key=True)
    name = Column(String)
    last_update = Column(DateTime)
    films = relationship("Film", back_populates="language")

class Actor(SakilaBase):
    __tablename__ = 'actor'
    actor_id = Column(Integer, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    last_update = Column(DateTime)
    film_actors = relationship("FilmActor", back_populates="actor")

class Category(SakilaBase):
    __tablename__ = 'category'
    category_id = Column(Integer, primary_key=True)
    name = Column(String)
    last_update = Column(DateTime)
    film_categories = relationship("FilmCategory", back_populates="category")

class Film(SakilaBase):
    __tablename__ = 'film'
    film_id = Column(Integer, primary_key=True)
    title = Column(String)
    description = Column(Text)
    release_year = Column(Integer) # Example has 2005, not YYYY format
    language_id = Column(Integer, ForeignKey('language.language_id'))
    length = Column(Integer)
    rating = Column(String)
    last_update = Column(DateTime)
    language = relationship("Language", back_populates="films")
    inventories = relationship("Inventory", back_populates="film")
    film_actors = relationship("FilmActor", back_populates="film")
    film_categories = relationship("FilmCategory", back_populates="film")

class FilmActor(SakilaBase):
    __tablename__ = 'film_actor'
    actor_id = Column(Integer, ForeignKey('actor.actor_id'), primary_key=True)
    film_id = Column(Integer, ForeignKey('film.film_id'), primary_key=True)
    last_update = Column(DateTime)
    actor = relationship("Actor", back_populates="film_actors")
    film = relationship("Film", back_populates="film_actors")

class FilmCategory(SakilaBase):
    __tablename__ = 'film_category'
    film_id = Column(Integer, ForeignKey('film.film_id'), primary_key=True)
    category_id = Column(Integer, ForeignKey('category.category_id'), primary_key=True)
    last_update = Column(DateTime)
    film = relationship("Film", back_populates="film_categories")
    category = relationship("Category", back_populates="film_categories")

class Inventory(SakilaBase):
    __tablename__ = 'inventory'
    inventory_id = Column(Integer, primary_key=True)
    film_id = Column(Integer, ForeignKey('film.film_id'))
    store_id = Column(Integer, ForeignKey('store.store_id'))
    last_update = Column(DateTime)
    film = relationship("Film", back_populates="inventories")
    store = relationship("Store", back_populates="inventories")
    rentals = relationship("Rental", back_populates="inventory")

class Rental(SakilaBase):
    __tablename__ = 'rental'
    rental_id = Column(Integer, primary_key=True)
    rental_date = Column(DateTime)
    inventory_id = Column(Integer, ForeignKey('inventory.inventory_id'))
    customer_id = Column(Integer, ForeignKey('customer.customer_id'))
    return_date = Column(DateTime)
    staff_id = Column(Integer, ForeignKey('staff.staff_id'))
    last_update = Column(DateTime)
    inventory = relationship("Inventory", back_populates="rentals")
    customer = relationship("Customer", back_populates="rentals")
    staff = relationship("Staff", back_populates="rentals")
    payments = relationship("Payment", back_populates="rental")

class Payment(SakilaBase):
    __tablename__ = 'payment'
    payment_id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey('customer.customer_id'))
    staff_id = Column(Integer, ForeignKey('staff.staff_id'))
    rental_id = Column(Integer, ForeignKey('rental.rental_id'))
    amount = Column(Numeric(5, 2))
    payment_date = Column(DateTime)
    last_update = Column(DateTime)
    customer = relationship("Customer", back_populates="payments")
    staff = relationship("Staff", back_populates="payments")
    rental = relationship("Rental", back_populates="payments")