from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker
from datetime import date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Date, Table, ForeignKey, Boolean
from sqlalchemy.orm import relationship, backref
from sqlalchemy.sql import func
from sqlalchemy import literal_column
from sqlalchemy import distinct

Base = declarative_base()


class Artist(Base):
    __tablename__ = 'artist'

    artist_id = Column(Integer, primary_key=True, nullable=False, autoincrement=True, unique=True)
    name = Column(String(45), nullable=False)
    description = Column(String(60), nullable=False)

    def __init__(self, name, description):
        super().__init__()
        self.name = name
        self.description = description


class Album(Base):
    __tablename__ = 'album'

    album_id = Column(Integer, primary_key=True, nullable=False, autoincrement=True, unique=True)
    title = Column(String(45), nullable=False)
    release_year = Column(Integer, nullable=False)

    def __init__(self, title, release_year):
        super().__init__()
        self.title = title
        self.release_year = release_year


class Track(Base):
    __tablename__ = 'track'

    track_id = Column(Integer, primary_key=True, nullable=False, autoincrement=True, unique=True)
    number_in_album = Column(Integer, nullable=False)
    name = Column(String(45), nullable=False)
    album_id = Column(Integer, ForeignKey('album.album_id'), nullable=False)
    album = relationship("Album", backref="track", uselist=False)
    length = Column(Integer, nullable=False)

    def __init__(self, number_in_album, name, album, length):
        super().__init__()
        self.number_in_album = number_in_album
        self.name = name
        self.album = album
        self.length = length


class Category(Base):
    __tablename__ = 'category'

    category_id = Column(Integer, primary_key=True, nullable=False, autoincrement=True, unique=True)
    name = Column(String(45), nullable=False, unique=True)

    def __init__(self, name):
        super().__init__()
        self.name = name


class Artist_Category(Base):
    __tablename__ = 'artist_category'

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True, unique=True)
    artist_id = Column(Integer, ForeignKey('artist.artist_id'), nullable=False)
    artist = relationship("Artist", backref="artist_category", uselist=False)
    category_id = Column(Integer, ForeignKey('category.category_id'), nullable=False)
    category = relationship("Category", backref="artist_category", uselist=False)

    def __init__(self, artist, category):
        super().__init__()
        self.artist = artist
        self.category = category


class Artist_Album(Base):
    __tablename__ = 'artist_album'

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True, unique=True)
    artist_id = Column(Integer, ForeignKey('artist.artist_id'), nullable=False)
    artist = relationship("Artist", backref="artist_album", uselist=False)
    album_id = Column(Integer, ForeignKey('album.album_id'), nullable=False)
    album = relationship("Album", backref="artist_album", uselist=False)

    def __init__(self, artist, album):
        super().__init__()
        self.artist = artist
        self.album = album


def request1(session):
    artists = session.query(Artist).all()
    print('Artists:')
    for artist in artists:
        print(f'{artist.artist_id}. {artist.name}. {artist.description} ')
    print('')


def request2(session):
    tracks = session.query(Track). \
        filter(Track.album_id == 5). \
        order_by(Track.number_in_album).all()
    print("Tracks in 5 album:")
    for track in tracks:
        print(
            f'track_id: {track.track_id}.  number on album: {track.number_in_album}. name: {track.name}. album_id: {track.album_id}. :track.length: {track.length}')
    print('')


def request3(session):
    albums = session.query(Album.title, func.count(Track.track_id).label("count_of_tracks")). \
        join(Track).filter(Album.release_year == 2007). \
        group_by(Album.album_id).all()
    print("Albums on 2007:")
    for album in albums:
        print(f'{album.title}. {album.count_of_tracks}')
    print('')


def request4(session):
    albums = session.query(Album.title, func.sum(Track.length).label('album_length')). \
        join(Track). \
        group_by(Album.title). \
        order_by(desc(literal_column('album_length'))). \
        limit(5)
    print("ТОР-5 albums by duration")
    for album in albums:
        print(
            f'Title: {album.title}. Length: {album.album_length}')
    print('')


def request5(session):
    max_albums = session.query(func.count(Album.album_id).label("count_of_album")).join(Artist_Album).join(
        Artist).group_by(Artist.artist_id).order_by(desc(literal_column("count_of_album"))).limit(1).scalar()
    artists = session.query(Artist.artist_id, Artist.name, Artist.description). \
        join(Artist_Album).join(Album). \
        group_by(Artist.artist_id).having(func.count(Album.album_id) == max_albums).all()

    print('Artists with max count of albums:')
    for artist in artists:
        print(f'{artist.artist_id}. {artist.name}. {artist.description}')
    print('')


def request6(session):
    durations = session.query(func.sum(Track.length).label('album_length')). \
        join(Album). \
        group_by(Album.album_id). \
        order_by(func.sum(Track.length)).all()

    min, max, sum, count = 10000, 0, 0, 0

    for duration in durations:
        if (min > duration.album_length):
            min = duration.album_length
        if (max < duration.album_length):
            max = duration.album_length
        sum += duration.album_length
        count += 1

    print(f'min duration of album: {min}')
    print(f'max duration of album: {max}')
    print(f'avg duration of album: {sum / count}')
    print('')


def add(session):
    artist = Artist(name='Bob Mary', description='The best artist of Canada')
    album = Album('END YEAR', 2023)
    artist_album = Artist_Album(artist, album)

    session.add(artist)
    session.add(album)
    session.add(artist_album)
    session.commit()


if __name__ == '__main__':
    engine = create_engine('mysql+mysqlconnector://root:752708@127.0.0.1/media_library')
    Session = sessionmaker(bind=engine)
    session = Session()

    request1(session)
    request2(session)
    request3(session)
    request4(session)
    request5(session)
    request6(session)

    # add(session)


