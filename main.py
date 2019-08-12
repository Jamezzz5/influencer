import sys
import logging
import argparse
import influencer.config as cfg
import influencer.export as exp

formatter = logging.Formatter('%(asctime)s [%(module)14s]' +
                              '[%(levelname)8s] %(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)

console = logging.StreamHandler(sys.stdout)
console.setFormatter(formatter)
log.addHandler(console)

log_file = logging.FileHandler('logfile.log', mode='w')
log_file.setFormatter(formatter)
log.addHandler(log_file)


def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logging.critical("Uncaught exception: ",
                     exc_info=(exc_type, exc_value, exc_traceback))


sys.excepthook = handle_exception

parser = argparse.ArgumentParser()
parser.add_argument('--nopull', action='store_true')
parser.add_argument('--append', action='store_true')
parser.add_argument('--filter', action='store_true')
parser.add_argument('--exp', choices=['all', 'db', 'ftp'])
args = parser.parse_args()


def main():
    if not args.nopull:
        config = cfg.Config(args.filter, args.append)
        config.do_all_jobs()
    if args.exp:
        exp_class = exp.ExportHandler()
        exp_class.export_loop(args.exp)


if __name__ == '__main__':
    main()
