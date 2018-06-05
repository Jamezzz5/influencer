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
        self.df = pd.DataFrame()
        self.full_config_path = config_path + config_file
        self.config = pd.read_excel(self.full_config_path)
        self.config = self.config.to_dict(orient='index')

    def do_all_jobs(self):
        api = ytapi.YtApi()
        for k, v in self.config.items():
            logging.info('Getting videos for the following query: {}'
                         .format(v))
            tdf = api.get_data(sd=v[self.sd], ed=v[self.ed],
                               query=v[self.query])
            self.df = self.df.append(tdf).reset_index(drop=True)
        utl.write_df(self.df, 'raw_videos.xlsx')
        df = self.filter_by_sponsored_videos('sponsored_videos.xlsx', self.df)
        utl.write_df(df, 'sponsored_videos.xlsx')

    @staticmethod
    def filter_by_sponsored_videos(raw_file=None, df=None):
        if raw_file:
            df = pd.read_excel(raw_file)
        df = df[~df['id'].duplicated()]
        filter_cols = ['description', 'title']
        df = utl.df_filter_by_word(df, filter_cols, 'sponsored')
        df = utl.df_filter_by_word(df, filter_cols, 'not sponsored', True)
        return df
