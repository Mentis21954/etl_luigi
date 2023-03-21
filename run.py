import luigi
import pandas as pd
import time
import requests
import json
import pymongo
from constants import LASTFM_API_KEY, DISCOGS_API_KEY


class extract_info_from_all_artists(luigi.Task):
    artist_names = luigi.ListParameter()

    def output(self):
        return luigi.LocalTarget('artist_contents.json')

    def run(self):
        artist_contents = {}
        for name in self.artist_names:
            url = ('https://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist=') + name + (
                '&api_key=') + LASTFM_API_KEY + '&format=json'
            artist_info = requests.get(url).json()
            artist_contents.update({name: artist_info['artist']['bio']['content']})
            print('Search description from lastfm.com for artist {} ...'.format(name))

        contents_df = pd.DataFrame(artist_contents.values(), columns=['Content'], index=artist_contents.keys())
        with self.output().open('w') as outfile:
            outfile.write(contents_df.to_json(orient='index'))


class clean_the_artist_content(luigi.Task):
    artist_names = luigi.ListParameter()

    def requires(self):
        return extract_info_from_all_artists(self.artist_names)

    def output(self):
        return luigi.LocalTarget(self.input().path)

    def run(self):
        # read the input file and store as a dataframe
        contents_df = pd.read_json(self.input().path, orient='index')
        # remove new line command and html tags
        contents_df['Content'] = contents_df['Content'].replace('\n', '', regex=True)
        contents_df['Content'] = contents_df['Content'].replace(r'<[^<>]*>', '', regex=True)
        print('Clean the informations texts')
        print(contents_df.head())

        with self.output().open('w') as outfile:
            outfile.write(contents_df.to_json(orient='index'))


class extract_titles_from_artist(luigi.Task):
    name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('{}_releases.json'.format(self.name))

    def run(self):
        # get the artist id from artist name
        url = 'https://api.discogs.com/database/search?q=' + self.name + ('&{?type=artist}&token=') + DISCOGS_API_KEY
        discogs_artist_info = requests.get(url).json()
        id = discogs_artist_info['results'][0]['id']

        print('Search releases from for discogs.com for artist ' + self.name + '...')

        # with id get artist's releases
        url = ('https://api.discogs.com/artists/') + str(id) + ('/releases')
        releases = requests.get(url).json()
        releases_df = pd.json_normalize(releases['releases'])

        # store the tracks info in a list
        title_info, colab_info, year_info, format_info, price_info = [], [], [], [], []
        for index, url in enumerate(releases_df['resource_url'].values):
            source = requests.get(url).json()
            # search if exists track's price
            if 'lowest_price' in source.keys():
                # print(str(index) + ': '+ str(source['title'])+ ' '+ str(source['lowest_price']))
                title_info.append(source['title'])
                colab_info.append(releases_df['artist'].iloc[index])
                year_info.append(source['year'])
                price_info.append(source['lowest_price'])
                if 'formats' in source.keys():
                    format_info.append(source['formats'][0]['name'])
                else:
                    format_info.append(None)
                print('Found ' + str((index + 1)) + ' titles!')

            # sleep 3 secs to don't miss requests
            time.sleep(3)

        print('Find tracks from artist ' + self.name + ' with Discogs ID: ' + str(id))
        with self.output().open('w') as outfile:
            outfile.write(json.dumps({'Title': title_info, 'Collaborations': colab_info,
                                      'Year': year_info, 'Format': format_info, 'Discogs Price': price_info}))


class remove_null_prices(luigi.Task):
    name = luigi.Parameter()

    def requires(self):
        return extract_titles_from_artist(self.name)

    def output(self):
        return luigi.LocalTarget(self.input().path)

    def run(self):
        # read the input file and store as a dataframe
        df = pd.read_json(self.input().path)
        # find and remove the rows/titles where there are no selling prices in discogs.com
        df = df[df['Discogs Price'].notna()]
        print('Remove tracks where there no selling price in discogs.com')
        with self.output().open('w') as outfile:
            outfile.write(df.to_json(orient='columns', compression='infer'))


class drop_duplicates_titles(luigi.Task):
    name = luigi.Parameter()

    def requires(self):
        return remove_null_prices(self.name)

    def output(self):
        return luigi.LocalTarget('{}_transformed_releases.json'.format(self.name))

    def run(self):
        # read the input file and store as a dataframe
        df = pd.read_json(self.input().path)
        # find and remove the duplicates titles
        df = df.drop_duplicates(subset=['Title'])
        print('find and remove the duplicates titles if exist')
        df = pd.DataFrame(data={'Collaborations': df['Collaborations'].values, 'Year': df['Year'].values,
                                'Format': df['Format'].values,
                                'Discogs Price': df['Discogs Price'].values}, index=(df['Title'].values))
        print(df.head())
        with self.output().open('w') as outfile:
            outfile.write(df.to_json(orient='index', compression='infer'))


class integrate_data(luigi.Task):
    artist_names = luigi.ListParameter()

    def requires(self):
        return {'artist_contents': clean_the_artist_content(self.artist_names),
                'artist_releases': drop_duplicates_titles(self.artist_names[0])}

    def output(self):
        return luigi.LocalTarget(self.input()['artist_releases'].path)
    def run(self):
        contents_df = pd.read_json(self.input()['artist_contents'].path, orient='index')
        print(contents_df.head())
        releases = {'Artist': self.artist_names[0],
                    'Description': contents_df['Content'][str(self.artist_names[0])]
                    }
        with self.input()['artist_releases'].open('r') as artist_releases_file:
            releases.update({'Releases': json.load(artist_releases_file)})

        print('Integrate the description and releases for artist {}'.format(self.artist_names[0]))
        with self.output().open('w') as outfile:
            outfile.write(json.dumps(releases))

class load_to_database(luigi.Task):
    artist_names = luigi.ListParameter()
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["mydatabase"]
    artists = db['artists']

    def requires(self):
        return integrate_data(artist_names=self.artist_names)

    def run(self):
        with self.input().open('r') as input_file:
            artist_info = json.load(input_file)
        self.artists.insert_one(artist_info)
        print('Artist {} insert to DataBase!'.format(artist_names[0]))



class Workflow(luigi.WrapperTask):
    artist_names = luigi.ListParameter()

    def requires(self):
        return integrate_data(artist_names=self.artist_names)


if __name__ == '__main__':
    df = pd.read_csv('spotify_artist_data.csv')
    artist_names = list(df['Artist Name'].unique())
    # luigi.build([extract_info_from_all_artists(artist_names[:2])], local_scheduler=True)
    # luigi.build([clean_the_artist_content(artist_names[:2])], local_scheduler=True)
    # luigi.build([drop_duplicates_titles(artist_names[0])], local_scheduler=True)
    luigi.build([load_to_database(artist_names=artist_names[:2])])
