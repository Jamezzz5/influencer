import logging
import pandas as pd
import api.utils as utl
import api.ytapi as ytapi


config_path = utl.config_path


class Config(object):
    def __init__(self, config_file='config.xlsx'):
        logging.info('Initalizing config file {}'.format(config_file))
        self.sd = 'StartDate'
        self.ed = 'EndDate'
        self.query = 'Query'
        self.game = 'Game'
        self.publisher = 'Publisher'
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
            logging.info('Getting videos for the following query: {}'
                         .format(v))
            tdf = api.get_video_data(sd=v[self.sd], ed=v[self.ed],
                                     query=v[self.query])
            for col in [self.game, self.publisher]:
                tdf[col] = v[col]
            self.df = self.df.append(tdf).reset_index(drop=True)
        utl.write_df(self.df, 'raw_videos.xlsx')
        self.df = self.filter_by_sponsored_videos('raw_videos.xlsx', self.df)
        utl.write_df(self.df, 'sponsored_videos.xlsx')
        return self.df

    def get_channels(self, api):
        df = pd.DataFrame()
        all_cids = list(self.df['channelId'].unique())
        cid_lists = [all_cids[i:i + 50] for i in range(0, len(all_cids), 50)]
        for idx, cids in enumerate(cid_lists):
            logging.info('Getting channel batch:'
                         '{} of {}.'.format(idx + 1, len(all_cids)))
            tdf = api.get_channel_data(cids)
            df = df.append(tdf, ignore_index=True)
        df.columns = ['channel{}'.format(x) for x in df.columns]
        self.df = self.df.merge(df, how='left', left_on='channelId',
                                right_on='channelid')
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
