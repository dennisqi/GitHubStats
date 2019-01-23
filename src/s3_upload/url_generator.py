import os
import datetime


def get_parse_datetime(last_line):
    substring = last_line.split('.')[-3]
    if substring:
        splited = substring.split('/')
        if splited and len(splited) > 1:
            datetimes = splited[1].split('-')
            return datetime.datetime(
                int(datetimes[0]), int(datetimes[1]),
                int(datetimes[2]), int(datetimes[3]))


def get_start_datetime(url_file):
    last_line = None
    if os.path.isfile(url_file):
        last_line = os.popen("tail -n 1 %s" % url_file).read().strip()
    if not last_line:
        return datetime.datetime(2011, 2, 11, 23)
    return get_parse_datetime(last_line)


def url_generator(start_datetime, end_datetime, unit, head_url, tail_url):
    while start_datetime < end_datetime:
        full_url = head_url \
            + str(start_datetime.year) + '-' \
            + '%02d' % start_datetime.month + '-' \
            + '%02d' % start_datetime.day + '-' \
            + str(start_datetime.hour) \
            + tail_url
        yield full_url
        start_datetime += unit


if __name__ == '__main__':

    url_file = '../../data/gharchive_urls.txt'
    updated_url_file = '../../data/updated_gharchive_urls.txt'
    head_url = 'http://data.gharchive.org/'
    tail_url = '.json.gz'

    unit_datetime = datetime.timedelta(hours=1)
    start_datetime = get_start_datetime(url_file)
    start_datetime += unit_datetime
    end_datetime = datetime.datetime.utcnow()
    end_datetime -= unit_datetime

    if start_datetime < end_datetime:
        with open(url_file, 'a') as f:
            with open(updated_url_file, 'w') as f_updated:
                for url in url_generator(
                        start_datetime, end_datetime,
                        unit_datetime, head_url, tail_url):
                    f.write(url + '\n')
                    f_updated.write(url + '\n')
                    print(url)
    else:
        open(updated_url_file, 'w').close()
