import sqlite3

def setup_tracks(db, data_file):
    '''(str, file) -> NoneType
    Create and populate the Tracks table with
    the data from the open file data_file.'''
    
    con = sqlite3.connect(db)
    
    cur = con.cursor()
    
    cur.execute('''CREATE TABLE Tracks (title TEXT, ID INTEGER, time INTEGER)''')
    

    for line in data_file.readlines()[1:]: 
        data = line.split(',')
        title = data[0].strip()
        ID = data[1].strip()
        time = data[2].strip()
        m, s = time.split(':')
        Time = int(m)*60 +int(s)
        
        cur.execute('''INSERT INTO Tracks VALUES(?, ?, ?)''', (title, ID, Time))
    
    con.commit()
    
    cur.close()
    
    con.close()


def setup_genres(db, data_file):
    '''(str, file) -> NoneType
    Create and populate the Genres table with
    the data from the open file data_file.'''
    
    con = sqlite3.connect(db)
    
    cur = con.cursor()
    
    cur.execute('''CREATE TABLE Genres (artist TEXT, genres TEXT)''')
    
    for line in data_file.readlines()[1:]: 
        data = line.split(',')
        artist = data[0].strip()
        genres = data[1].strip()
        
        cur.execute('''INSERT INTO Genres VALUES(?, ?)''', (artist, genres))
    
    con.commit()
    
    cur.close()
    
    con.close()
    

def setup_albums(db, data_file):
    '''(str, file) -> NoneType
    Create and populate the Albums table with
    the data from the open file data_file.'''
    
    con = sqlite3.connect(db)
    
    cur = con.cursor()
    
    cur.execute('''CREATE TABLE Albums (ID INTEGER, artist TEXT, album TEXT)''')
    
    for line in data_file.readlines()[1:]: 
        data = line.split(',')
        ID = data[0].strip()
        artist = data[1].strip()
        album = data[2].strip()
        
        cur.execute('''INSERT INTO Albums VALUES(?, ?, ?)''', (ID, artist, album))
    
    con.commit()
    
    cur.close()
    
    con.close()
    
def setup_popularity(db):
    '''(str) -> None
    The parameter is a database filename. Create and populate the 
    Popularity table with the given database. 
    ''' 
    con = sqlite3.connect(db)
    
    cur = con.cursor()
    
    cur.execute('''CREATE TABLE Popularity AS SELECT Artist FROM Albums''')
    
    cur.execute('''ALTER TABLE Popularity ADD Hits INT NOT NULL DEFAULT 0''')
   
    con.commit()
    
    cur.close()
    
    con.close()

def run_query(db, query, args=None):
    '''Return the results of running query q on database db.
    If given, args contains the query arguments.'''

    # create connection
    con = sqlite3.connect(db)
    # create cursor
    cur = con.cursor()
    # run the query
    # case when no args passed
    if args == None:
        cur.execute(query)
    else:
        # args has a value which should be a tuple
        cur.execute(query, args)
        
    # fetch result
    result = cur.fetchall()
    # close everything
    cur.close()
    con.close()
    return result

def run_command(db, query, args=None):
    '''Return the results of running query q on database db.
    If given, args contains the query arguments.'''

    # create connection
    con = sqlite3.connect(db)
    # create cursor
    cur = con.cursor()
    # run the query
    # case when no args passed
    if args == None:
        cur.execute(query)
    else:
        # args has a value which should be a tuple
        cur.execute(query, args)
        
    # commit result
    con.commit()
    # close everything
    cur.close()
    con.close()

    
def update_popularity(db, artist):
    '''(str, str) -> None
    Update the number of times the given artist/band has 
    had a track searched for in the popularity table (add 1 to the hit value).
    '''
    query = "UPDATE Popularity SET HITS = HITS + 1 WHERE Popularity.artist = ?"
    
    return run_command(db, query, (artist,))
    
def get_albums(db, artist):
    '''(str, str) -> list of tuple
    The first parameter is a database name and the second an artist or band's 
    name. Return the unique album titles, track titles and ids produced by the artist/band.
    '''
    
    query = "SELECT * FROM Albums WHERE artist = ?"
    return run_query(db, query, (artist,))

def get_greatest(db):
    '''(str) -> list of tuple
    The parameter is a database name. Return a list of tuples containing 
    all those unique artist names whose albums have the word 'Greatest' in their title. 
    '''
    query = 'SELECT DISTINCT artist FROM Albums WHERE album LIKE "%Greatest%"'
    return run_query(db, query)


def get_genres(db, album):
    '''(str, str) -> list of tuple
    The first parameter is a database name, the second is an album.  Return
    the artists that produced the album and the genres associated with that 
    artist. It's possible for more than one artist to have the same album title 
    and for an artist to belong to more than one genres.
    '''
    query = "SELECT Genres.artist, Genres.genres FROM Genres JOIN Albums ON Genres.artist=Albums.artist WHERE album = ?"
    return run_query(db, query, (album,))

def get_track_info(db, title):#
    '''(str, int) -> list of tuple
    The first parameter is a database name and the second is a track title.
    Return the track's title, ID, band/artist, album name and time. This
    function should update the popularity table by adding 1 to the 
    Hits field of the corresponding artist/band.
    '''
    a = "SELECT * FROM Tracks JOIN Albums ON Tracks.ID = Albums.ID WHERE title = ?"
    b = "SELECT artist FROM Albums JOIN Tracks ON Albums.ID = Tracks.ID WHERE title = ?"
    update_popularity(db, b)
    
    return run_query(db, a, (title,))
    
def get_album_lengths(db):
    '''(str) -> list of tuples
    The parameter is a database name. Return a list of tuples 
    containing each artist name, album title and total length
    of the album.
    '''
    a = "SELECT DISTINCT artist, album, sum(time) FROM Albums JOIN Tracks ON Tracks.ID = Albums.ID GROUP BY Albums.Album "
    return run_query(db, a)

def multiple_albums(db):#
    '''(str) -> list of tuple
    The parameter is a database name. Return a list of tuples of artists 
    with more than one album. 
    '''
    a = "SELECT artist, album FROM Albums"
    return run_query(db, a)

def get_dict_of_artists(db, artist_list):
    '''(str, list of tuple) -> dict of dict
    Given a database name and a list of tuples containing artist/band 
    names (for example [('The Beatles',), ('The Tragically Hip', )]). 
    Return a dictionary whose keys are artist/band names and 
    values are dictionaries with key equal to album title and values 
    a list of track titles (ie, a list of str not a list of tup).
    '''
    empty1 = []
    empty2 = []
    emptyd = {}
    
    query = '''SELECT album, GROUP_CONCAT(title) FROM Albums JOIN Tracks ON Albums.ID = Tracks.ID WHERE artist = ? GROUP BY album'''
    
    for item in artist_list:
        run_query(db,query,(item,))
        empty1.append(run_query(db,query,(item,)))
        for thing in empty1: 
            for other_thing in thing:
                emptyd[other_thing[0]]=other_thing[1]
            empty2.append(emptyd)
            emptyd={}
            empty1 = []
   
   
    return dict(zip(artist_list, empty2))
    


def get_num_tracks(db):
    '''(str) -> list of tuple
    GIven a database, return a list of tuples of the artist
    names and number of tracks produced by that artist
    '''
    query = '''SELECT DISTINCT artist, COUNT(title) FROM Albums JOIN Tracks ON Tracks.ID = Albums.ID GROUP BY Albums.artist'''
    return run_query(db, query)


        
def get_popularity(db):
    '''(str) -> list of tuple
    Return the artists and hits from the popularity table.
    '''
    return run_query(db, '''SELECT artist, Hits FROM Popularity''')

