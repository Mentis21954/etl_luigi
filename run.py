import luigi
import pandas as pd
import requests
import time
import json
import pymongo

LASTFM_API_KEY = '3f8f9f826bc4b0c8b529828839d38e4b'
DISCOGS_API_KEY = 'hhNKFVCSbBWJATBYMyIxxjCJDSuDZMBGnCapdhOy'


class extract_info_from_artist(luigi.Task):
    name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('data/{}/{}_content.json'.format(self.name, self.name))

    def run(self):
        artist_contents = {}
        url = 'https://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist=' + self.name + '&api_key=' + LASTFM_API_KEY + '&format=json'
        artist_info = requests.get(url).json()
        artist_contents.update({self.name: {'Content': artist_info['artist']['bio']['content']}})
        print('Search description from lastfm.com for artist {} ...'.format(self.name))

        with self.output().open('w') as outfile:
            outfile.write(json.dumps(artist_contents))


class extract_titles_from_artist(luigi.Task):
    name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('data/{}/{}_releases.json'.format(self.name, self.name))

    def run(self):
        # get the artist id from artist name
        url = 'https://api.discogs.com/database/search?q=' + self.name + '&{?type=artist}&token=' + DISCOGS_API_KEY
        discogs_artist_info = requests.get(url).json()
        id = discogs_artist_info['results'][0]['id']

        print('Search releases from discogs.com for artist {} ...'.format(str(self.name)))

        # with id get artist's releases
        url = ('https://api.discogs.com/artists/') + str(id) + ('/releases')
        releases = requests.get(url).json()

        print('Found releases from artist ' + self.name + ' with Discogs ID: ' + str(id))

        with self.output().open('w') as outfile:
            outfile.write(json.dumps({self.name: releases['releases']}))


class extract_info_for_titles(luigi.Task):
    name = luigi.Parameter()

    def requires(self):
        return extract_titles_from_artist(self.name)

    def output(self):
        return luigi.LocalTarget('data/{}/{}_releases_info.json'.format(self.name, self.name))

    def run(self):
        # read releases file to find info for each release title
        with self.input().open('r') as releases_file:
            releases = json.load(releases_file)

        # store the releases/tracks info in a list
        releases_info = []
        for index in range(len(releases[self.name])):
            url = releases[self.name][index]['resource_url']
            params = {'token': DISCOGS_API_KEY}
            source = requests.get(url, params=params).json()
            # search if exists track's price
            if 'lowest_price' in source.keys():
                if 'formats' in source.keys():
                    releases_info.append({'Title': source['title'],
                                          'Collaborations': releases[self.name][index]['artist'],
                                          'Year': source['year'],
                                          'Format': source['formats'][0]['name'],
                                          'Discogs Price': source['lowest_price']})
                else:
                    releases_info.append({'Title': source['title'],
                                          'Collaborations': releases[self.name][index]['artist'],
                                          'Year': source['year'],
                                          'Format': None,
                                          'Discogs Price': source['lowest_price']})
                print('Found informations from discogs.com for title {}'.format(source['title']))
                # sleep 4 secs to don't miss requests
                time.sleep(4)

        with self.output().open('w') as outfile:
            outfile.write(json.dumps(releases_info))


class extract_playcounts_from_titles_by_artist(luigi.Task):
    name = luigi.Parameter()

    def requires(self):
        return extract_titles_from_artist(self.name)

    def output(self):
        return luigi.LocalTarget('data/{}/{}_playcounts.json'.format(self.name, self.name))

    def run(self):
        # read releases file to find listeners
        with self.input().open('r') as releases_file:
            releases = json.load(releases_file)

        # initialize list for playcounts for each title
        playcounts = []
        for index in range(len(releases[self.name])):
            title = releases[self.name][index]['title']
            url = 'https://ws.audioscrobbler.com/2.0/?method=track.getInfo&api_key=' + LASTFM_API_KEY + '&artist=' + self.name + '&track=' + title + '&format=json'

            try:
                source = requests.get(url).json()
                if 'track' in source.keys():
                    playcounts.append({'Title': source['track']['name'],
                                       'Lastfm Playcount': source['track']['playcount']})
                    print('Found playcount from last.fm for title {}'.format(title))
                else:
                    print('Not found playcount from last.fm for title {}'.format(title))
            except:
                print('Not found playcount from last.fm for title {}'.format(title))
                continue

        with self.output().open('w') as outfile:
            outfile.write(json.dumps(playcounts))


class clean_the_artist_content(luigi.Task):
    name = luigi.Parameter()

    def requires(self):
        return extract_info_from_artist(self.name)

    def output(self):
        return luigi.LocalTarget(self.input().path)

    def run(self):
        # read the input file and store as a dataframe
        content_df = pd.read_json(self.input().path, orient='index')
        # remove new line commands, html tags and "", ''
        content_df['Content'] = content_df['Content'].replace(r'\r+|\n+|\t+', '', regex=True)
        content_df['Content'] = content_df['Content'].replace(r'<[^<>]*>', '', regex=True)
        content_df['Content'] = content_df['Content'].replace(r'"', '', regex=True)
        content_df['Content'] = content_df['Content'].replace(r"'", '', regex=True)
        print('Clean the informations texts')

        with self.output().open('w') as outfile:
            outfile.write(content_df.to_json(orient='index'))


class remove_wrong_values(luigi.Task):
    name = luigi.Parameter()

    def requires(self):
        return extract_info_for_titles(self.name)

    def output(self):
        return luigi.LocalTarget(self.input().path)

    def run(self):
        # read the input file and store as a dataframe
        df = pd.read_json(self.input().path)
        # find and remove the rows/titles where there are no selling prices in discogs.com
        df = df[df['Discogs Price'].notna()]
        print('Remove releases where there no selling price in discogs.com')
        # keep only the rows has positive value of year
        df = df[df['Year'] > 0]
        print('Remove releases where there no selling price in discogs.com')

        with self.output().open('w') as outfile:
            outfile.write(df.to_json(orient='columns', compression='infer'))


class merge_titles_data(luigi.Task):
    name = luigi.Parameter()

    def requires(self):
        return {'playcounts': extract_playcounts_from_titles_by_artist(self.name),
                'artist_releases': remove_wrong_values(self.name)}

    def output(self):
        return luigi.LocalTarget(self.input()['artist_releases'].path)

    def run(self):
        releases_df = pd.read_json(self.input()['artist_releases'].path)
        playcounts_df = pd.read_json(self.input()['playcounts'].path)

        df = pd.merge(releases_df, playcounts_df, on='Title')
        print('Merge releases and playcounts data for artist {}'.format(self.name))

        with self.output().open('w') as outfile:
            outfile.write(df.to_json(orient='records', compression='infer'))


class drop_duplicates_titles(luigi.Task):
    name = luigi.Parameter()

    def requires(self):
        return merge_titles_data(self.name)

    def output(self):
        return luigi.LocalTarget(self.input().path)

    def run(self):
        df = pd.read_json(self.input().path)
        df = df.drop_duplicates(subset=['Title'])
        print('find and remove the duplicates titles if exist')
        df = df.set_index('Title')

        with self.output().open('w') as outfile:
            outfile.write(df.to_json(orient='index', compression='infer'))


class integrate_data(luigi.Task):
    name = luigi.Parameter()

    def requires(self):
        return {'artist_content': clean_the_artist_content(self.name),
                'artist_releases': drop_duplicates_titles(self.name)}

    def output(self):
        return luigi.LocalTarget('data/{}/{}.json'.format(self.name, self.name))

    def run(self):
        # read specific artist content file
        with self.input()['artist_content'].open('r') as artist_content_file:
            content = json.load(artist_content_file)

        # read specific artist releases file
        with self.input()['artist_releases'].open('r') as artist_releases_file:
            # add the releases to content dict
            content.update({self.name: {'Description': content[self.name]['Content'],
                                        'Releases': json.load(artist_releases_file)}})
        print('Integrate the description and releases for artist {}'.format(self.name))
        with self.output().open('w') as outfile:
            outfile.write(json.dumps({self.name: content[self.name]}))


class load_to_database(luigi.Task):
    artist_names = luigi.ListParameter()
    client = pymongo.MongoClient(
        "mongodb+srv://user:AotD8lF0WspDIA4i@cluster0.qtikgbg.mongodb.net/?retryWrites=true&w=majority")
    db = client["mydatabase"]
    artists = db['artists']

    def requires(self):
        for name in self.artist_names:
            yield integrate_data(name)

    def run(self):
        data = {}
        for name in self.artist_names:
            with open('data/{}/{}.json'.format(name, name)) as artist_file:
                data.update(json.load(artist_file))

        for artist in list(data.keys()):
            self.artists.insert_one({'Artist': str(artist),
                                     'Description': data[str(artist)]['Description'],
                                     'Releases': data[str(artist)]['Releases']
                                     })
            print('--- Artist {} insert to DataBase! ---'.format(artist))


if __name__ == '__main__':
    df = pd.read_csv('spotify_artist_data.csv')
    artist_names = list(df['Artist Name'].unique())

    luigi.build([load_to_database(artist_names=artist_names[:4])], workers=8)
