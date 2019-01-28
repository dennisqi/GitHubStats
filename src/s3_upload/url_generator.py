import os
import datetime
import subprocess


class UrlFileGenerator(object):
    """
    Generates the urls of files that are not in S3.
    Stores them into data/coming_urls.txt.
    """

    def __init__(
        self,
        saved_urls_file='../../data/saved_urls.txt',
        coming_urls_file='../../data/coming_urls.txt',
        default_start_datetime=datetime.datetime(2011, 2, 11, 23)
    ):
        self.saved_urls_file = saved_urls_file
        self.coming_urls_file = coming_urls_file
        f = open(coming_urls_file, 'w')
        f.close()
        self.default_start_datetime = default_start_datetime

    def get_start_datetime(self):
        latest_line = None
        if os.path.isfile(self.saved_urls_file):
            output = subprocess.Popen(
                ["tail", "-n", "1", self.saved_urls_file],
                stdout=subprocess.PIPE
            ).communicate()[0]
            latest_line = output.strip()
        if not latest_line:
            return self.default_start_datetime
        return self.parse_datetime(latest_line)

    def parse_datetime(self, last_line):
        substring = last_line.split('.')[-3]
        if substring:
            splited = substring.split('/')
            if splited and len(splited) > 1:
                datetimes = splited[1]
                return self.parse(datetimes)
        raise ValueError('The latest url in latest_url_file is not valid.')

    def parse(self, string):
        splited = string.split('-')
        if splited:
            return datetime.datetime(
                int(splited[0]), int(splited[1]),
                int(splited[2]), int(splited[3]))
        raise ValueError('The latest url in latest_url_file is not valid.')

    def url_generator(self, start_datetime, end_datetime,
                      unit, head_url, tail_url):
        while start_datetime < end_datetime:
            full_url = head_url \
                + str(start_datetime.year) + '-' \
                + '%02d' % start_datetime.month + '-' \
                + '%02d' % start_datetime.day + '-' \
                + str(start_datetime.hour) \
                + tail_url
            yield full_url
            start_datetime += unit

    def write_url(self, start_datetime,
                  url_head='http://data.gharchive.org/', url_tail='.json.gz',
                  end_datetime=datetime.datetime.utcnow(),
                  unit_datetime=datetime.timedelta(hours=1)
                  ):
        start_datetime += unit_datetime
        end_datetime -= unit_datetime
        if start_datetime < end_datetime:
            for url in self.url_generator(
                    start_datetime, end_datetime,
                    unit_datetime, url_head, url_tail):
                with open(self.coming_urls_file, 'a') as cuf:
                    cuf.write(url + '\n')
                print(url)


if __name__ == '__main__':
    url_file_generator = UrlFileGenerator()
    start_datetime = url_file_generator.get_start_datetime()
    url_file_generator.write_url(start_datetime)
