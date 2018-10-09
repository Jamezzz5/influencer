import sys
import json
import time
import logging
import requests
import pandas as pd
import datetime as dt
import influencer.utils as utl
from requests_oauthlib import OAuth2Session

config_path = utl.config_path
search_base_url = 'https://www.googleapis.com/youtube/v3/search?part=snippet'
vid_base_url = ('https://www.googleapis.com/youtube/v3/videos?'
                'part=snippet,statistics')
channel_base_url = ('https://www.googleapis.com/youtube/v3/channels?'
                    'part=snippet%2CcontentDetails%2Cstatistics')


class YtApi(object):
    def __init__(self, config='ytapi.json'):
        self.config = config
        self.config_file = None
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        self.refresh_token = None
        self.refresh_url = None
        self.config_list = None
        self.client = None
        self.df = pd.DataFrame()
        self.r = None
        if config:
            self.input_config(config)

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  ' +
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading YT config file: {}'.format(config))
        self.config_file = config_path + config
        self.load_config()
        self.check_config()
        self.config_file = config_path + config

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.config_file))
            sys.exit(0)
        self.client_id = self.config['client_id']
        self.client_secret = self.config['client_secret']
        self.access_token = self.config['access_token']
        self.refresh_token = self.config['refresh_token']
        self.refresh_url = self.config['refresh_url']
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.refresh_token, self.refresh_url]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in YT config file.'
                                'Aborting.'.format(item))
                sys.exit(0)

    def get_client(self):
        token = {'access_token': self.access_token,
                 'refresh_token': self.refresh_token,
                 'token_type': 'Bearer',
                 'expires_in': 3600,
                 'expires_at': 1504135205.73}
        extra = {'client_id': self.client_id,
                 'client_secret': self.client_secret}
        self.client = OAuth2Session(self.client_id, token=token)
        token = self.get_token(token, extra)
        self.client = OAuth2Session(self.client_id, token=token)

    def get_token(self, token, extra):
        try:
            token = self.client.refresh_token(token_url=self.refresh_url,
                                              **extra)
        except requests.exceptions.SSLError as e:
            logging.warning('Warning SSLError as follows {}'.format(e))
            time.sleep(30)
            token = self.get_token(token, extra)
        return token

    def make_request(self, url):
        self.get_client()
        self.r = self.client.get(url)
        return self.r

    @staticmethod
    def date_check(date):
        if str(date) == 'nan':
            date = None
        else:
            date = dt.datetime.strftime(date, '%Y-%m-%dT00:00:00Z')
        return date

    def get_data_default_check(self, sd, ed):
        sd = self.date_check(sd)
        ed = self.date_check(ed)
        if sd and ed and sd > ed:
            logging.warning('Start date greater than end date.  Start date' +
                            'was set to end date.')
            sd = ed
        return sd, ed

    @staticmethod
    def create_search_url(query, sd, ed, base_url=search_base_url):
        max_url = '&maxResults=50'
        type_url = '&type=video'
        query_url = '&q={}'.format(query)
        full_url = base_url + max_url + type_url + query_url
        if sd:
            sd_url = '&publishedAfter={}'.format(sd)
            full_url += sd_url
        if ed:
            ed_url = '&publishedBefore={}'.format(ed)
            full_url += ed_url
        return full_url

    @staticmethod
    def create_yt_url(ids, base_url=vid_base_url):
        vid_url = '&id={}'.format(','.join(ids))
        full_url = base_url + vid_url
        return full_url

    def get_video_data(self, sd=None, ed=None, query=None):
        sd, ed = self.get_data_default_check(sd, ed)
        self.get_raw_video_data(query, sd, ed)
        return self.df

    def get_raw_video_data(self, query, sd, ed):
        vid_ids = self.get_vid_ids(query, sd, ed)
        tdf = self.make_request_get_df(vid_ids, vid_base_url)
        tdf['query'] = query
        self.df = self.df.append(tdf)

    def get_vid_ids(self, query, sd, ed):
        full_url = self.create_search_url(query, sd, ed)
        self.r = self.make_request(full_url)
        df = self.data_to_df(self.r, 'items', ['snippet', 'id'])
        vid_ids = list(df['videoId'])
        return vid_ids

    def make_request_get_df(self, ids, base_url):
        full_url = self.create_yt_url(ids, base_url)
        self.r = self.make_request(full_url)
        df = self.data_to_df(self.r, 'items', ['statistics', 'snippet'])
        return df

    def get_channel_data(self, cids):
        self.df = pd.DataFrame()
        self.df = self.make_request_get_df(cids, channel_base_url)
        return self.df

    @staticmethod
    def data_to_df(r, main_key, nested_fields):
        data = r.json()[main_key]
        df = pd.DataFrame(data)
        for col in nested_fields:
            tdf = pd.DataFrame(list(df[col]))
            drop_cols = [x for x in df.columns if x in tdf.columns]
            if drop_cols:
                df.drop(drop_cols, axis=1, inplace=True)
            df = df.join(tdf)
            df.drop([col], axis=1, inplace=True)
        return df
