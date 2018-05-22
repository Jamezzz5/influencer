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
        self.write_df()

    def write_df(self):
        self.df.to_excel('raw_videos.xlsx', index=False)
