import mysql.connector
import datetime


def request1(cnx):
    cursor = cnx.cursor()
    query = ("SELECT * FROM artist")
    cursor.execute(query)
    print("#1. Вывести информацию обо всех артистах: \n")
    for (artist_id, name, description) in cursor:
        print("{}. {}. {}".format(
            artist_id, name, description))

    cursor.close()


def request2(cnx):
    cursor = cnx.cursor()
    print("\n Enter the album number ")
    album = int(input())
    query = ("""SELECT * FROM track
             WHERE album_id=%s ORDER BY number_in_album;""" % (album,))
    cursor.execute(query)

    print("#2. Вывести информацию обо всех треках в указанном альбоме, упорядочить по номеру.\n")
    print(f"Tracks in album {album}")
    for (track_id, number_in_album, name, album_id, length) in cursor:
        print(
            f"id: {track_id}. number_in_album: {number_in_album}. name: {name}. album_id: {album_id}. length: {length}")
    cursor.close()


def request3(cnx):
    cursor = cnx.cursor()
    print("\n Enter the release_year ")
    release_year = int(input())
    query = ("""SELECT title as "Альбом", count(track_id) as "Количество треков" FROM album
            JOIN track using(album_id)
            WHERE release_year = %s
            GROUP BY title;""" % (release_year,))
    cursor.execute(query)

    print(
        "#3. Вывести информацию обо всех альбомах с указанием количества треков в альбоме, выпущенных в указанный год.\n")
    print(f"Альбомы и количество их треков в {release_year}")
    for title, count in cursor:
        print(f"Альбом: {title}, Количество треков: {count}")
    cursor.close()


def request4(cnx):
    cursor = cnx.cursor()
    query = ("""SELECT title as "Альбом", sum(length) as "Продолжительность" FROM album
        JOIN track using(album_id)
        GROUP BY title
        ORDER BY sum(length) desc 
        limit 5;""")
    cursor.execute(query)

    print("#4. Вывести топ 5 альбомов по продолжительности.\n")
    for title, length in cursor:
        print(f"Title: {title}, length: {length}")
    cursor.close()

# def request5(cnx):
#     cursor = cnx.cursor()
#     query = ("""SELECT name as 'Артист', count(album_id) as "Количество альбомов" FROM Artist_album
#     JOIN album using(album_id)
#     JOIN artist using(artist_id)
#     GROUP BY artist_id
#     ORDER BY count(album_id) desc;""")
#     cursor.execute(query)
#
#     print("#5. Вывести артистов с максимальным количеством альбомов.\n")
#     for artist, count in cursor:
#         print(f"{artist}, {count}")
#     cursor.close()

def request5(cnx):
    cursor = cnx.cursor()
    query = ("""SELECT artist.artist_id, artist.name, count(album_id) AS count_albums FROM artist
        JOIN artist_album using(artist_id)
        JOIN album using(album_id)
        GROUP BY artist.artist_id
        HAVING count_albums=
        (SELECT count(album_id) AS count_albums FROM artist
        JOIN artist_album using(artist_id)
        JOIN album using(album_id)
        GROUP BY artist.artist_id
        ORDER BY count(album_id) DESC
        limit 1);""")
    cursor.execute(query)

    print("#5. Вывести артистов с максимальным количеством альбомов.\n")
    for artist,name, count in cursor:
        print(f"{artist}, {name}, {count}")
    cursor.close()

# def request6(cnx):
#     cursor = cnx.cursor()
#     query = ("""SELECT title as "Альбом", min(length) as "MIN", avg(length) as "AVERAGE", max(length) as "MAX" FROM album
#     JOIN track using(album_id)
#     GROUP BY title""")
#     cursor.execute(query)
#
#     print("#6. Вывести информацию о минимальной, средней и максимальной продолжительности альбомов.\n")
#     for title, min, avg, max in cursor:
#         print(f"Title: {title} \t Min: {min}\t Avg: {avg}\t Max: {max}")
#     cursor.close()

def request6(cnx):
    cursor = cnx.cursor()
    query = ("""SELECT min(album_length) as min_length,avg(album_length) as avg_length,max(album_length) as max_length FROM 
        (SELECT sum(track.length) as album_length FROM album
        JOIN track using(album_id)
        GROUP BY (album_id)) as album_length;""")
    cursor.execute(query)

    print("#6. Вывести информацию о минимальной, средней и максимальной продолжительности альбомов.\n")
    for min, avg, max in cursor:
        print(f"Min: {min}\t Avg: {avg}\t Max: {max}")
    cursor.close()


if __name__ == '__main__':
    cnx = mysql.connector.connect(user='root', password='752708',
                                  host='127.0.0.1',
                                  database='media_library')

    request1(cnx)
    request2(cnx)
    request3(cnx)
    request4(cnx)
    request5(cnx)
    request6(cnx)

    cnx.close()
