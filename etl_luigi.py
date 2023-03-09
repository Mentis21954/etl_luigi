import luigi
import pandas as pd
import time
import requests
import json
from constants import LASTFM_API_KEY, DISCOGS_API_KEY


class extract_info_from_all_artists(luigi.Task):
    artist_names = luigi.ListParameter()

    def output(self):
        return luigi.LocalTarget('artist_contents.json')

    def run(self):
        artist_contents = {}
        for name in self.artist_names:
            url = ('https://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist=') + name + (
                '&api_key=') + LASTFM_API_KEY + ('&format=json')
            artist_info = requests.get(url).json()
            artist_contents.update({name: artist_info['artist']['bio']['content']})
            print('Search infrmation for artist {} ...'.format(name))

        with self.output().open('w') as outfile:
            outfile.write(json.dumps(artist_contents))


class extract_titles_from_artist(luigi.Task):
    name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('{}_releases.json'.format(self.name))

    def run(self):
        # get the artist id from artist name
        url = ('https://api.discogs.com/database/search?q=') + self.name + ('&{?type=artist}&token=') + DISCOGS_API_KEY
        discogs_artist_info = requests.get(url).json()
        id = discogs_artist_info['results'][0]['id']

        print('Search titles from artist ' + self.name + '...')

        # with id get artist's releases
        url = ('https://api.discogs.com/artists/') + str(id) + ('/releases')
        releases = requests.get(url).json()
        releases_df = pd.json_normalize(releases['releases'])

        # store the tracks info in a list
        tracks_info = []
        for index, url in enumerate(releases_df['resource_url'].values):
            source = requests.get(url).json()
            # search if exists track's price
            if 'lowest_price' in source.keys():
                # print(str(index) + ': '+ str(source['title'])+ ' '+ str(source['lowest_price']))
                if 'formats' in source.keys():
                    tracks_info.append((source['title'], releases_df['artist'].iloc[index], source['year'],
                                        source['formats'][0]['name'], source['lowest_price']))
                else:
                    tracks_info.append(
                        (source['title'], releases_df['artist'].iloc[index], source['year'], None,
                         source['lowest_price']))
                print('Found ' + str((index + 1)) + ' titles!')

            # sleep 3 secs to don't miss requests
            time.sleep(3)

        print('Find tracks from artist ' + self.name + ' with Discogs ID: ' + str(id))
        with self.output().open('w') as outfile:
            outfile.write(json.dumps({'Artist': self.name, 'Track Info': tracks_info}))

if __name__ == '__main__':
    df = pd.read_csv('spotify_artist_data.csv')
    artist_names = list(df['Artist Name'].unique())
    luigi.build([extract_info_from_all_artists(artist_names[:2])], local_scheduler=True)
    luigi.build([extract_titles_from_artist(artist_names[0]), extract_titles_from_artist(artist_names[1])], local_scheduler=True)
