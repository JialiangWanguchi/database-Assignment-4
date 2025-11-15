# This file implements the 4 CLI commands: Init, Full-load, Incremental, and Validate.

import click
import datetime
from sqlalchemy import func, exc, and_, text  # Added 'text' import for raw SQL
from database import AnalyticsBase, sqlite_engine, SakilaSession, AnalyticsSession
from models_sakila import (
    Customer,
    Address,
    City,
    Country,
    Store,
    Staff,
    Film,
    Language,
    Actor,
    Category,
    FilmActor,
    FilmCategory,
    Inventory,
    Rental,
    Payment,
)
from models_analytics import (
    DimCustomer,
    DimStore,
    DimFilm,
    DimActor,
    DimCategory,
    DimDate,
    BridgeFilmActor,
    BridgeFilmCategory,
    FactRental,
    FactPayment,
    SyncState,
)

# Helper Function for Date Dimension
def populate_dim_date(a_session, start_date, end_date):
    """
    Pre-populates the DimDate table with a range of dates.
    This is a common Data Warehouse practice.
    """
    click.echo("Populating dim_date table...")
    current_date = start_date
    dates = []
    while current_date <= end_date:
        is_weekend = current_date.weekday() >= 5  # 5 = Saturday, 6 = Sunday
        quarter = (current_date.month - 1) // 3 + 1

        dates.append(
            DimDate(
                date_key=int(current_date.strftime("%Y%m%d")),  #
                date=current_date,
                year=current_date.year,
                quarter=quarter,
                month=current_date.month,
                day_of_month=current_date.day,
                day_of_week=current_date.weekday(),
                is_weekend=is_weekend,
            )
        )
        current_date += datetime.timedelta(days=1)

    try:
        a_session.bulk_save_objects(dates)
        a_session.commit()
        click.echo(f"Populated dim_date with {len(dates)} dates.")
    except exc.IntegrityError:
        a_session.rollback()
        click.echo("dim_date already populated.")
    except Exception as e:
        a_session.rollback()
        click.echo(f"Error populating dim_date: {e}", err=True)

# Shared init logic (extracted for reuse)
def perform_init():
    """Core init logic: create tables, populate dates, init sync_state, add indexes."""
    click.echo("Initializing analytics database...")
    AnalyticsBase.metadata.create_all(sqlite_engine)

    with AnalyticsSession() as session:
        if session.query(DimDate).count() == 0:
            populate_dim_date(
                session, datetime.date(2005, 1, 1), datetime.date(2006, 12, 31)
            )

        # Initialize the sync_state table
        tables_to_sync = [
            "customer",
            "store",
            "film",
            "actor",
            "category",
            "rental",
            "payment",
            "film_actor",
            "film_category",
        ]
        for table in tables_to_sync:
            exists = session.query(SyncState).filter_by(table_name=table).first()
            if not exists:
                initial_sync = SyncState(
                    table_name=table, last_sync_timestamp=datetime.datetime(2000, 1, 1)
                )
                session.add(initial_sync)
        session.commit()

        # Create useful indexes for performance
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_fact_rental_date_rented ON fact_rental (date_key_rented)",
            "CREATE INDEX IF NOT EXISTS idx_fact_rental_film ON fact_rental (film_key)",
            "CREATE INDEX IF NOT EXISTS idx_fact_rental_store ON fact_rental (store_key)",
            "CREATE INDEX IF NOT EXISTS idx_fact_rental_customer ON fact_rental (customer_key)",
            "CREATE INDEX IF NOT EXISTS idx_fact_payment_date_paid ON fact_payment (date_key_paid)",
            "CREATE INDEX IF NOT EXISTS idx_fact_payment_store ON fact_payment (store_key)",
            "CREATE INDEX IF NOT EXISTS idx_fact_payment_customer ON fact_payment (customer_key)",
        ]
        for idx_sql in indexes:
            session.execute(text(idx_sql))  # Wrapped in text() for SQLAlchemy 2.x
        session.commit()

    click.echo("Database initialized successfully.")

# CLI Command Group
@click.group()
def cli():
    """A CLI tool for the Sakila data sync assignment."""
    pass


# Init Command
@cli.command()
def init():
    """[Init] Initializes the SQLite database and all analytics tables."""
    try:
        perform_init()
    except Exception as e:
        click.echo(f"Init failed: {e}", err=True)


# Full-Load Command
@cli.command()
@click.option(
    "--force", is_flag=True, help="Force reload by deleting all existing data."
)
def full_load(force):
    """[Full-load] Performs a complete import from Sakila to SQLite."""
    click.echo("Starting full load...")

    s_session = SakilaSession()
    a_session = AnalyticsSession()

    if force:
        click.echo("Force flag set. Deleting all existing analytics data...")
        AnalyticsBase.metadata.drop_all(sqlite_engine)  # Efficient drop
        a_session.commit()
        click.echo("Re-initializing dim_date and sync_state...")
        perform_init()  # Now calls shared function, no Click issue

    # In-memory ID maps for mapping Natural Keys to Surrogate Keys
    customer_key_map = {}
    store_key_map = {}
    film_key_map = {}
    actor_key_map = {}
    category_key_map = {}
    date_key_map = {d.date: d.date_key for d in a_session.query(DimDate)}

    def get_date_key(date_obj):
        """Helper to safely get a date_key from a date or datetime."""
        if date_obj is None:
            return None
        return date_key_map.get(date_obj.date(), None)

    try:
        click.echo("Loading Dimensions...")

        # DimCustomer
        all_customers = (
            s_session.query(Customer).join(Address).join(City).join(Country).all()
        )
        for c in all_customers:
            dim_c = DimCustomer(
                customer_key=c.customer_id * 100 + 1,
                customer_id=c.customer_id,
                first_name=c.first_name,
                last_name=c.last_name,
                active=c.active,
                city=c.address.city.city,
                country=c.address.city.country.country,
                last_update=c.last_update,
            )
            a_session.add(dim_c)
        a_session.flush()
        for dim_c in a_session.query(DimCustomer):
            customer_key_map[dim_c.customer_id] = dim_c.customer_key

        # DimStore
        all_stores = s_session.query(Store).join(Address).join(City).join(Country).all()
        for s in all_stores:
            dim_s = DimStore(
                store_key=s.store_id * 100 + 1,
                store_id=s.store_id,
                city=s.address.city.city,
                country=s.address.city.country.country,
                last_update=s.last_update,
            )
            a_session.add(dim_s)
        a_session.flush()
        for dim_s in a_session.query(DimStore):
            store_key_map[dim_s.store_id] = dim_s.store_key

        # DimFilm
        all_films = s_session.query(Film).join(Language).all()
        for f in all_films:
            dim_f = DimFilm(
                film_key=f.film_id * 100 + 1,
                film_id=f.film_id,
                title=f.title,
                rating=f.rating,
                length=f.length,
                language=f.language.name,
                release_year=f.release_year,
                last_update=f.last_update,
            )
            a_session.add(dim_f)
        a_session.flush()
        for dim_f in a_session.query(DimFilm):
            film_key_map[dim_f.film_id] = dim_f.film_key

        # DimActor
        all_actors = s_session.query(Actor).all()
        for a in all_actors:
            dim_a = DimActor(
                actor_key=a.actor_id * 100 + 1,
                actor_id=a.actor_id,
                first_name=a.first_name,
                last_name=a.last_name,
                last_update=a.last_update,
            )
            a_session.add(dim_a)
        a_session.flush()
        for dim_a in a_session.query(DimActor):
            actor_key_map[dim_a.actor_id] = dim_a.actor_key

        # DimCategory
        all_categories = s_session.query(Category).all()
        for c in all_categories:
            dim_c = DimCategory(
                category_key=c.category_id * 100 + 1,
                category_id=c.category_id, 
                name=c.name,
                last_update=c.last_update
            )
            a_session.add(dim_c)
        a_session.flush()
        for dim_c in a_session.query(DimCategory):
            category_key_map[dim_c.category_id] = dim_c.category_key

        a_session.commit()
        click.echo("Dimensions loaded.")

        # Load Bridges
        click.echo("Loading Bridges...")

        # BridgeFilmActor
        all_film_actors = s_session.query(FilmActor).all()
        for fa in all_film_actors:
            film_key = film_key_map.get(fa.film_id)
            actor_key = actor_key_map.get(fa.actor_id)
            if film_key and actor_key:
                existing = (
                    a_session.query(BridgeFilmActor)
                    .filter_by(film_key=film_key, actor_key=actor_key)
                    .first()
                )
                if not existing:
                    a_session.add(BridgeFilmActor(film_key=film_key, actor_key=actor_key))

        # BridgeFilmCategory
        all_film_categories = s_session.query(FilmCategory).all()
        for fc in all_film_categories:
            film_key = film_key_map.get(fc.film_id)
            category_key = category_key_map.get(fc.category_id)
            if film_key and category_key:
                existing = (
                    a_session.query(BridgeFilmCategory)
                    .filter_by(film_key=film_key, category_key=category_key)
                    .first()
                )
                if not existing:
                    a_session.add(BridgeFilmCategory(film_key=film_key, category_key=category_key))

        a_session.commit()
        click.echo("Bridges loaded.")

        # Load Facts
        click.echo("Loading Facts...")

        # FactRental
        all_rentals = s_session.query(Rental).join(Inventory).all()
        for r in all_rentals:
            duration = None
            if r.return_date and r.rental_date:
                duration = (r.return_date - r.rental_date).days

            existing = a_session.query(FactRental).filter_by(rental_id=r.rental_id).first()
            if not existing:
                fact_r = FactRental(
                    rental_id=r.rental_id,
                    date_key_rented=get_date_key(r.rental_date),
                    date_key_returned=get_date_key(r.return_date),
                    film_key=film_key_map.get(r.inventory.film_id),
                    store_key=store_key_map.get(r.inventory.store_id),
                    customer_key=customer_key_map.get(r.customer_id),
                    staff_id=r.staff_id,
                    rental_duration_days=duration,
                )
                a_session.add(fact_r)

        # FactPayment
        all_payments = s_session.query(Payment).join(Staff).all()
        for p in all_payments:
            existing = a_session.query(FactPayment).filter_by(payment_id=p.payment_id).first()
            if not existing:
                fact_p = FactPayment(
                    payment_id=p.payment_id,
                    date_key_paid=get_date_key(p.payment_date),
                    customer_key=customer_key_map.get(p.customer_id),
                    store_key=store_key_map.get(p.staff.store_id),
                    staff_id=p.staff_id,
                    amount=float(p.amount),
                )
                a_session.add(fact_p)

        a_session.commit()
        click.echo("Facts loaded.")

        # Update SyncState
        current_time = datetime.datetime.now()
        for state in a_session.query(SyncState).all():
            state.last_sync_timestamp = current_time
        a_session.commit()

        click.echo("Full load complete.")

    except Exception as e:
        a_session.rollback()
        click.echo(f"Error during full load: {e}", err=True)
    finally:
        s_session.close()
        a_session.close()


# Incremental Command
@cli.command()
def incremental():
    """[Incremental] Loads only new or changed data since last sync."""
    click.echo("Starting incremental update...")

    s_session = SakilaSession()
    a_session = AnalyticsSession()

    try:
        current_sync_time = datetime.datetime.now()

        customer_key_map = {
            c.customer_id: c.customer_key for c in a_session.query(DimCustomer)
        }
        store_key_map = {s.store_id: s.store_key for s in a_session.query(DimStore)}
        film_key_map = {f.film_id: f.film_key for f in a_session.query(DimFilm)}
        actor_key_map = {a.actor_id: a.actor_key for a in a_session.query(DimActor)}
        category_key_map = {
            c.category_id: c.category_key for c in a_session.query(DimCategory)
        }
        date_key_map = {d.date: d.date_key for d in a_session.query(DimDate)}

        def get_date_key(date_obj):
            if date_obj is None:
                return None
            return date_key_map.get(date_obj.date(), None)

        # Sync Dimensions

        # DimCustomer
        state = a_session.query(SyncState).filter_by(table_name="customer").first()
        last_sync = state.last_sync_timestamp
        click.echo(f"Syncing customers updated since {last_sync}...")
        updated_customers = (
            s_session.query(Customer)
            .join(Address)
            .join(City)
            .join(Country)
            .filter(Customer.last_update > last_sync)
            .all()
        )
        for c in updated_customers:
            existing = (
                a_session.query(DimCustomer)
                .filter_by(customer_id=c.customer_id)
                .first()
            )
            if existing:
                existing.first_name = c.first_name
                existing.last_name = c.last_name
                existing.active = c.active
                existing.city = c.address.city.city
                existing.country = c.address.city.country.country
                existing.last_update = c.last_update
            else:
                new_c = DimCustomer(
                    customer_key=c.customer_id * 100 + 1,
                    customer_id=c.customer_id,
                    first_name=c.first_name,
                    last_name=c.last_name,
                    active=c.active,
                    city=c.address.city.city,
                    country=c.address.city.country.country,
                    last_update=c.last_update,
                )
                a_session.add(new_c)
                a_session.flush()
                customer_key_map[new_c.customer_id] = new_c.customer_key
        state.last_sync_timestamp = current_sync_time

        # DimStore
        state = a_session.query(SyncState).filter_by(table_name="store").first()
        last_sync = state.last_sync_timestamp
        click.echo(f"Syncing stores updated since {last_sync}...")
        updated_stores = (
            s_session.query(Store)
            .join(Address)
            .join(City)
            .join(Country)
            .filter(Store.last_update > last_sync)
            .all()
        )
        for s in updated_stores:
            existing = a_session.query(DimStore).filter_by(store_id=s.store_id).first()
            if existing:
                existing.city = s.address.city.city
                existing.country = s.address.city.country.country
                existing.last_update = s.last_update
            else:
                new_s = DimStore(
                    store_key=s.store_id * 100 + 1,
                    store_id=s.store_id,
                    city=s.address.city.city,
                    country=s.address.city.country.country,
                    last_update=s.last_update,
                )
                a_session.add(new_s)
                a_session.flush()
                store_key_map[new_s.store_id] = new_s.store_key
        state.last_sync_timestamp = current_sync_time

        # DimFilm
        state = a_session.query(SyncState).filter_by(table_name="film").first()
        last_sync = state.last_sync_timestamp
        click.echo(f"Syncing films updated since {last_sync}...")
        updated_films = (
            s_session.query(Film)
            .join(Language)
            .filter(Film.last_update > last_sync)
            .all()
        )
        for f in updated_films:
            existing = a_session.query(DimFilm).filter_by(film_id=f.film_id).first()
            if existing:
                existing.title = f.title
                existing.rating = f.rating
                existing.length = f.length
                existing.language = f.language.name
                existing.release_year = f.release_year
                existing.last_update = f.last_update
            else:
                new_f = DimFilm(
                    film_key=f.film_id * 100 + 1,
                    film_id=f.film_id,
                    title=f.title,
                    rating=f.rating,
                    length=f.length,
                    language=f.language.name,
                    release_year=f.release_year,
                    last_update=f.last_update,
                )
                a_session.add(new_f)
                a_session.flush()
                film_key_map[new_f.film_id] = new_f.film_key
        state.last_sync_timestamp = current_sync_time

        # DimActor
        state = a_session.query(SyncState).filter_by(table_name="actor").first()
        last_sync = state.last_sync_timestamp
        click.echo(f"Syncing actors updated since {last_sync}...")
        updated_actors = (
            s_session.query(Actor).filter(Actor.last_update > last_sync).all()
        )
        for a in updated_actors:
            existing = a_session.query(DimActor).filter_by(actor_id=a.actor_id).first()
            if existing:
                existing.first_name = a.first_name
                existing.last_name = a.last_name
                existing.last_update = a.last_update
            else:
                new_a = DimActor(
                    actor_key=a.actor_id * 100 + 1,
                    actor_id=a.actor_id,
                    first_name=a.first_name,
                    last_name=a.last_name,
                    last_update=a.last_update,
                )
                a_session.add(new_a)
                a_session.flush()
                actor_key_map[new_a.actor_id] = new_a.actor_key
        state.last_sync_timestamp = current_sync_time

        # DimCategory
        state = a_session.query(SyncState).filter_by(table_name="category").first()
        last_sync = state.last_sync_timestamp
        click.echo(f"Syncing categories updated since {last_sync}...")
        updated_categories = (
            s_session.query(Category).filter(Category.last_update > last_sync).all()
        )
        for c in updated_categories:
            existing = (
                a_session.query(DimCategory)
                .filter_by(category_id=c.category_id)
                .first()
            )
            if existing:
                existing.name = c.name
                existing.last_update = c.last_update
            else:
                new_c = DimCategory(
                    category_key=c.category_id * 100 + 1,
                    category_id=c.category_id, 
                    name=c.name,
                    last_update=c.last_update,  # Fixed typo from last_sync_timestamp
                )
                a_session.add(new_c)
                a_session.flush()
                category_key_map[new_c.category_id] = new_c.category_key
        state.last_sync_timestamp = current_sync_time

        # Sync Bridges

        # BridgeFilmActor
        state = a_session.query(SyncState).filter_by(table_name="film_actor").first()
        last_sync = state.last_sync_timestamp
        click.echo(f"Syncing film_actor bridge updated since {last_sync}...")
        new_film_actors = (
            s_session.query(FilmActor).filter(FilmActor.last_update > last_sync).all()
        )
        for fa in new_film_actors:
            film_key = film_key_map.get(fa.film_id)
            actor_key = actor_key_map.get(fa.actor_id)
            if film_key and actor_key:
                existing = (
                    a_session.query(BridgeFilmActor)
                    .filter_by(film_key=film_key, actor_key=actor_key)
                    .first()
                )
                if not existing:
                    a_session.add(
                        BridgeFilmActor(film_key=film_key, actor_key=actor_key)
                    )
        state.last_sync_timestamp = current_sync_time

        # BridgeFilmCategory
        state = a_session.query(SyncState).filter_by(table_name="film_category").first()
        last_sync = state.last_sync_timestamp
        click.echo(f"Syncing film_category bridge updated since {last_sync}...")
        new_film_categories = (
            s_session.query(FilmCategory)
            .filter(FilmCategory.last_update > last_sync)
            .all()
        )
        for fc in new_film_categories:
            film_key = film_key_map.get(fc.film_id)
            category_key = category_key_map.get(fc.category_id)
            if film_key and category_key:
                existing = (
                    a_session.query(BridgeFilmCategory)
                    .filter_by(film_key=film_key, category_key=category_key)
                    .first()
                )
                if not existing:
                    a_session.add(
                        BridgeFilmCategory(film_key=film_key, category_key=category_key)
                    )
        state.last_sync_timestamp = current_sync_time

        # Sync Facts (Insert-Only Logic)

        # FactRental (using rental_date as marker )
        state = a_session.query(SyncState).filter_by(table_name="rental").first()
        last_sync = state.last_sync_timestamp
        click.echo(f"Syncing rentals created since {last_sync}...")
        new_rentals = (
            s_session.query(Rental)
            .join(Inventory)
            .filter(Rental.rental_date > last_sync)
            .all()
        )
        for r in new_rentals:
            existing = (
                a_session.query(FactRental).filter_by(rental_id=r.rental_id).first()
            )
            if not existing:
                duration = None
                if r.return_date and r.rental_date:
                    duration = (r.return_date - r.rental_date).days

                fact_r = FactRental(
                    rental_id=r.rental_id,
                    date_key_rented=get_date_key(r.rental_date),
                    date_key_returned=get_date_key(r.return_date),
                    film_key=film_key_map.get(r.inventory.film_id),
                    store_key=store_key_map.get(r.inventory.store_id),
                    customer_key=customer_key_map.get(r.customer_id),
                    staff_id=r.staff_id,
                    rental_duration_days=duration,
                )
                a_session.add(fact_r)
        state.last_sync_timestamp = current_sync_time

        # FactPayment (using payment_date as marker )
        state = a_session.query(SyncState).filter_by(table_name="payment").first()
        last_sync = state.last_sync_timestamp
        click.echo(f"Syncing payments created since {last_sync}...")
        new_payments = (
            s_session.query(Payment)
            .join(Staff)
            .filter(Payment.payment_date > last_sync)
            .all()
        )
        for p in new_payments:
            existing = (
                a_session.query(FactPayment).filter_by(payment_id=p.payment_id).first()
            )
            if not existing:
                fact_p = FactPayment(
                    payment_id=p.payment_id,
                    date_key_paid=get_date_key(p.payment_date),
                    customer_key=customer_key_map.get(p.customer_id),
                    store_key=store_key_map.get(p.staff.store_id),
                    staff_id=p.staff_id,
                    amount=float(p.amount),
                )
                a_session.add(fact_p)
        state.last_sync_timestamp = current_sync_time

        # Commit the entire incremental transaction
        a_session.commit()
        click.echo("Incremental update complete.")

    except Exception as e:
        a_session.rollback()  #
        click.echo(f"Error during incremental update: {e}", err=True)
    finally:
        s_session.close()
        a_session.close()


# Validate Command
@cli.command()
@click.option("--days", default=30, type=int, help="Number of days to validate. ")
def validate(days):
    """[Validate] Verifies data consistency between source and target."""
    click.echo(f"Validating data consistency for the last {days} days... ")

    s_session = SakilaSession()
    a_session = AnalyticsSession()

    try:
        # Calculate date range
        validation_date = datetime.date.today()
        start_date = validation_date - datetime.timedelta(days=days)

        click.echo("Dimension Counts (Total)")

        # 1. Validate Dimension Counts (Total)
        sakila_cust_count = s_session.query(Customer).count()
        analytics_cust_count = a_session.query(DimCustomer).count()
        click.echo(
            f"Customer Count: Sakila={sakila_cust_count}, Analytics={analytics_cust_count}"
        )
        if sakila_cust_count != analytics_cust_count:
            click.echo("Validation FAILED: Customer counts do not match.", err=True)
            return
        else:
            click.echo("Validation SUCCESS: Customer counts match.")

        sakila_film_count = s_session.query(Film).count()
        analytics_film_count = a_session.query(DimFilm).count()
        click.echo(
            f"Film Count: Sakila={sakila_film_count}, Analytics={analytics_film_count}"
        )
        if sakila_film_count != analytics_film_count:
            click.echo("Validation FAILED: Film counts do not match.", err=True)
            return
        else:
            click.echo("Validation SUCCESS: Film counts match.")

        click.echo(f"Fact Counts & Totals (Last {days} Days)")

        # 2. Validate Fact Counts (Last N Days) with precise date filter
        sakila_rental_count = (
            s_session.query(func.count(Rental.rental_id))
            .join(Inventory)
            .filter(func.date(Rental.rental_date) >= start_date)
            .scalar() or 0
        )
        analytics_rental_count = (
            a_session.query(func.count(FactRental.rental_id))
            .join(DimDate, DimDate.date_key == FactRental.date_key_rented)
            .filter(DimDate.date >= start_date)
            .scalar() or 0
        )
        click.echo(
            f"Rental Count: Sakila={sakila_rental_count}, Analytics={analytics_rental_count}"
        )
        if sakila_rental_count != analytics_rental_count:
            click.echo("Validation FAILED: Rental counts do not match.", err=True)
            return
        else:
            click.echo("Validation SUCCESS: Rental counts match.")

        # 3. Validate Fact Totals (Last N Days) with precise date filter
        sakila_payment_total = (
            s_session.query(func.sum(Payment.amount))
            .join(Staff)
            .filter(func.date(Payment.payment_date) >= start_date)
            .scalar() or 0.0
        )
        analytics_payment_total = (
            a_session.query(func.sum(FactPayment.amount))
            .join(DimDate, DimDate.date_key == FactPayment.date_key_paid)
            .filter(DimDate.date >= start_date)
            .scalar() or 0.0
        )
        click.echo(
            f"Payment Total: Sakila=${sakila_payment_total:.2f}, Analytics=${analytics_payment_total:.2f}"
        )
        if abs(float(sakila_payment_total) - analytics_payment_total) >= 0.01:
            click.echo("Validation FAILED: Payment totals do not match.", err=True)
            return
        else:
            click.echo("Validation SUCCESS: Payment totals match.")

        # 4. Validate per-store rentals count (Last N Days)
        sakila_rental_per_store = dict(
            s_session.query(Store.store_id, func.count(Rental.rental_id))
            .join(Inventory).join(Rental)
            .filter(func.date(Rental.rental_date) >= start_date)
            .group_by(Store.store_id)
            .all()
        )
        analytics_rental_per_store = dict(
            a_session.query(DimStore.store_id, func.count(FactRental.rental_id))
            .join(FactRental, FactRental.store_key == DimStore.store_key).join(DimDate, DimDate.date_key == FactRental.date_key_rented)
            .filter(DimDate.date >= start_date)
            .group_by(DimStore.store_id)
            .all()
        )
        click.echo("Per-Store Rental Counts:")
        mismatch = False
        for store_id, count in sakila_rental_per_store.items():
            ana_count = analytics_rental_per_store.get(store_id, 0)
            click.echo(f"  Store {store_id}: Sakila={count}, Analytics={ana_count}")
            if count != ana_count:
                mismatch = True
        if mismatch:
            click.echo("Validation FAILED: Per-store rental counts mismatch.", err=True)
            return
        else:
            click.echo("Validation SUCCESS: Per-store rental counts match.")

        # 5. Validate per-store payment totals (Last N Days)
        sakila_payment_per_store = dict(
            s_session.query(Store.store_id, func.sum(Payment.amount))
            .join(Staff).join(Payment)
            .filter(func.date(Payment.payment_date) >= start_date)
            .group_by(Store.store_id)
            .all()
        )
        analytics_payment_per_store = dict(
            a_session.query(DimStore.store_id, func.sum(FactPayment.amount))
            .join(FactPayment, FactPayment.store_key == DimStore.store_key).join(DimDate, DimDate.date_key == FactPayment.date_key_paid)
            .filter(DimDate.date >= start_date)
            .group_by(DimStore.store_id)
            .all()
        )
        click.echo("Per-Store Payment Totals:")
        mismatch = False
        for store_id, total in sakila_payment_per_store.items():
            ana_total = analytics_payment_per_store.get(store_id, 0.0)
            click.echo(f"  Store {store_id}: Sakila=${total or 0:.2f}, Analytics=${ana_total:.2f}")
            if abs((total or 0) - ana_total) >= 0.01:
                mismatch = True
        if mismatch:
            click.echo("Validation FAILED: Per-store payment totals mismatch.", err=True)
            return
        else:
            click.echo("Validation SUCCESS: Per-store payment totals match.")

        click.echo("Overall validation PASSED!")

    except Exception as e:
        click.echo(f"Error during validation: {e}", err=True)
    finally:
        s_session.close()
        a_session.close()


if __name__ == "__main__":
    cli()
