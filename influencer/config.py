import logging
import pandas as pd
import datetime as dt
import influencer.utils as utl
import influencer.ytapi as ytapi


config_path = utl.config_path


class Config(object):
    def __init__(self, sponsor_filter=True, append=False, config_file='config.xlsx'):
        logging.info('Initializing config file {}'.format(config_file))
        self.sponsor_filter = sponsor_filter
        self.append = append
        self.sd = 'StartDate'
        self.ed = 'EndDate'
        self.query = 'Query'
        self.game = 'Game'
        self.publisher = 'Publisher'
        self.url = 'Url'
        self.df = pd.DataFrame()
        self.full_config_path = config_path + config_file
        self.config = pd.read_excel(self.full_config_path)
        self.config = self.config.to_dict(orient='index')

    def do_all_jobs(self):
        api = ytapi.YtApi()
        self.get_videos(api)
        self.get_channels(api)

    def get_videos(self, api):
        for k, v in self.config.items():
            logging.info('Getting video {}'.format(v))
            if 'Url' in v and v['Url']:
                video_id = v['Url'].split('watch?v=')[1]
                tdf = api.make_request_get_df([video_id])
            else:
                tdf = api.get_video_data(sd=v[self.sd], ed=v[self.ed],
                                         query=v[self.query])
            for col in [self.game, self.publisher]:
                tdf[col] = v[col]
            self.df = pd.concat([self.df, tdf], ignore_index=True)
        self.df['videoeventdate'] = dt.datetime.today()
        if self.append:
            try:
                df = pd.read_excel('raw_videos.xlsx')
            except FileNotFoundError:
                df = pd.DataFrame()
            self.df = pd.concat([df, self.df], ignore_index=True)
        utl.write_df(self.df, 'raw_videos.xlsx')
        if self.sponsor_filter:
            self.df = self.filter_by_sponsored_videos('raw_videos.xlsx', self.df)
        utl.write_df(self.df, 'sponsored_videos.xlsx')
        return self.df

    def get_channels(self, api):
        df = pd.DataFrame()
        all_cids = list(self.df['channelId'].unique())
        cid_lists = [all_cids[i:i + 50] for i in range(0, len(all_cids), 50)]
        for idx, cids in enumerate(cid_lists):
            logging.info('Getting channel batch: '
                         '{} of {}.'.format(idx + 1, len(cid_lists)))
            tdf = api.get_channel_data(cids)
            df = pd.concat([df, tdf], ignore_index=True)
        df.columns = ['channel{}'.format(x) for x in df.columns]
        self.df = self.df.merge(df, how='left', left_on='channelId',
                                right_on='channelid')
        self.df['channeleventdate'] = dt.datetime.today()
        self.df['channelurl'] = ('https://www.youtube.com/channel/' +
                                 self.df['channelId'])
        utl.write_df(self.df, 'sponsored_videos.xlsx')
        return self.df

    @staticmethod
    def filter_by_sponsored_videos(raw_file=None, df=None):
        if raw_file:
            df = pd.read_excel(raw_file)
        df = df[~df['id'].duplicated()]
        filter_cols = ['description', 'title']
        df = utl.df_filter_by_word(df, filter_cols, 'sponsored')
        df = utl.df_filter_by_word(df, filter_cols, 'not sponsored', True)
        return df
