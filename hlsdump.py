"""
HLS download

This does not work for encrypted files (yet)

A cool trick to get a working video from these dumps

    ls video*.ts | sort -t '-' -k 2n  | xargs cat > joinedfile
    mplayer -demuxer lavf -correct-pts joinedfile

"""
from __future__ import print_function
import hls
import sys
import requests
import time
import os
# TODO: override user agent

class HLSDownloader:
    """
    An HLS helper to dump a stream into a file
    """

    DEBUG = False

    def __init__(self, url, folder):
        self.folder = folder
        try:
            os.mkdir(folder)
        except OSError:
            pass
#        for filename in os.listdir(folder):
#            if filename.startswith('video-'):
#                raise RuntimeError('path %s already has some files in it' % folder )

        self.playlist = url
        # The next sequence num we want
        self.nextseq = -1

        # Stats from last playlist fetch
        self.segmentcount = 0
        self.targetduration = 0
        self.last_waittime = 0
        self.skipped = 0
        self.waitfactor = 0.5

        # Global stats
        self.notfound = 0
        self.lostcount = 0

    def fetch_playlist(self):
        """
        Download a playlist and fetch all segments that were not
        previously downloaded
        """
        if self.DEBUG:
            print('')

        skip_wait = False
        strm = hls.get_stream(self.playlist)
        if self.nextseq == -1 or strm.sequence >= self.nextseq:
            start = 0
        else:
            start = self.nextseq - strm.sequence
        self.skipped = float(start) / len(strm.segment_urls)
        self.segmentcount = len(strm.segment_urls)
        self.targetduration = strm.info.target_duration

        if self.nextseq != -1 and strm.sequence > self.nextseq:
            self.lostcount += strm.sequence - self.nextseq

        for i in range(start, len(strm.segment_urls)):
            self.print_progress(strm, i)
            try:
                self.save_segment(strm.segment_urls[i], strm.sequence + i)
            except requests.HTTPError, ex:
                if ex.response.status_code == 404:
                    self.notfound += 1
                    skip_wait = True
                    continue
                else:
                    raise ex

        self.nextseq = strm.sequence+len(strm.segment_urls)
        if skip_wait:
            return 0
        else:
            return self.waittime(strm, len(strm.segment_urls)-start)

    def waittime(self, strm, segmentcount):
        """
        Returns time you should wait before calling fetch_playlist again

        * strm is a MediaStream object
        * segmentcount is the number of segments you played
        """
        # There is nothing scientific about this function
        # If you need GOOD timing considerations for HLS
        # check the RFC
        if self.skipped >= 0.8:
            self.waitfactor = 1
        elif self.skipped <= 0.01:
            self.waitfactor = 0.5

        wtime = strm.info.target_duration * self.waitfactor

	return strm.info.target_duration * 0.5
        # Make sure waittime is between 
        # target_duration and ~targetduration*#segments
#        return min(max(wtime, strm.info.target_duration), 0.9*strm.info.target_duration*len(strm.segment_urls))

    def print_progress(self, strm, segment):
        """
        Show information about current stream/chunk and
        overall statistics
        """
        if not self.DEBUG:
            sys.stdout.write("\r")
        sys.stdout.write("\r#%d Skipped:%d%% Wait:%.1f Wf:%.1f" % (strm.sequence + segment, self.skipped*100, self.last_waittime, self.waitfactor))
        sys.stdout.write(" Seg#:%d Dur:%d Lost:%d NotFound:%d" % (self.segmentcount, self.targetduration, self.lostcount, self.notfound))
        if self.DEBUG:
            print('')
        sys.stdout.flush()

    def save_segment(self, url, num, chunk_size=1024*32):
        """
        Dowload URL and write it into the out file object

        * chunk_size is the amount of content we hold in memory
        """
        req = requests.get( url, stream=True)
        req.raise_for_status()
        with file(os.path.join(self.folder, 'video-%d.ts' % num ), 'wb') as out:
            for chunk in req.iter_content(chunk_size):
                out.write(chunk)

    def download(self):
        """
        Block while downloading HLS stream
        """
        while True:
            try:
                waittime = self.fetch_playlist()
                self.last_waittime = waittime
                time.sleep(waittime)
            except requests.exceptions.ConnectionError, ex:
                print(ex)
                time.sleep(2)

def main():
    if len(sys.argv) != 3:
        print('Usage: hlsdump <path> <url>')
        sys.exit(-1)

    try:
        dump = HLSDownloader(sys.argv[2], sys.argv[1])
        dump.DEBUG = True
        dump.download()
    except KeyboardInterrupt:
        print("\nInterrupted")
    except RuntimeError, ex:
        print(ex)

if __name__ == '__main__':
    main()
