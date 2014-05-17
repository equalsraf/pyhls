"""
HLS download

* This does not work for encrypted files (yet)

Basically works as follows

1. The main thread (hls_playlist_loop()) periodically polls the playlist, and
   queues segment urls
2. A background thread worker (HLSSegmentDownloader) reads the queue, downloads
   the segments and saves then into individual files
3: Another background thread handles some HTTP retries

"""
from __future__ import print_function
from __future__ import unicode_literals
import hls
import sys
import requests
from requests.exceptions import ConnectionError
import time
import os
import hashlib
import threading
import Queue
import socket

def retry_save_segment_loop(queue, http_session):
    """
    Background worker to retry failed segment downloads, this
    is similar to HLSSegmentDownloader.save_segment() but
    without retries and timeouts
    """
    while True:
        try:
            url, path = queue.get()
            req = http_session.get( url, stream=True)

            if os.path.exists(path):
                print('[Retry] File %s already exists, skipping' % (path))
                continue
            with file( path, 'wb') as out:
                for chunk in req.iter_content():
                    out.write(chunk)
        except:
            continue
        print('[Retry] Successfully retrieved deferred segment as %s' % path)

class HLSSegmentDownloader(threading.Thread):
    """
    Dowloads HLS video segments

    The stats as shown by print_info() refer to

    failed: amount of segments we failed to get due to errors
            e.g. HTTP 404, timeouts, etc
    total:  amount of segments we tried to retrieve
    missed: amount of segments that we did not get because there
            were gaps in the playlist
    """

    def __init__(self, queue, folder='video', http_session=requests.Session()):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.queue = queue
        self.folder = folder
        self.http_session = http_session
        self.name_prefix = 'video-'

        # Create folder if it does not exist
        if not os.path.exists(self.folder):
            os.mkdir(self.folder)
        
        if not os.path.isdir(self.folder):
            raise RuntimeError('%s is not a folder' % self.folder)

        self.lastseq = -1
        self.stats = {
                'failed': 0,
                'missed': 0,
                'total': 0
                }
        
        # Start background retry worker
        self.retry_queue = Queue.Queue()
        retry_thread = threading.Thread(target=retry_save_segment_loop, 
                                        args=(self.retry_queue, self.http_session))
        retry_thread.setDaemon(True)
        retry_thread.start()

    def run(self):
        while True:
            seq, url, playlistinfo = self.queue.get()
            self.stats['total'] += 1
            if self.lastseq != -1 and seq != self.lastseq+1:
                self.stats['missed'] += seq - self.lastseq

            self.save_segment(url, seq, playlistinfo)
            self.lastseq = seq
            self.queue.task_done()
            self.print_info(seq, playlistinfo)

    @staticmethod
    def remove(path):
        """
        The sames as os.remove() except it ignores raised
        exceptions
        """
        try:
            os.remove(path)
        except OSError:
            pass

    def save_segment(self, url, seq, playlistinfo, chunk_size=1024*32):
        """
        Dowload segment URL and save it into a file. The file will
        be named as

            <prefix><segment sequence number>.ts

        * url of the segment
        * seq number of the segment 
        * playlistinfo is a MediaInfo object for the playlist
        * chunk_size is the amount of content we hold in memory at a time

        You can set the **name_prefix** attribute to change the prefix
        """

        path = os.path.join(self.folder, 
                            '%s-%d.ts' % (self.name_prefix, seq) )
        try:
            # FIXME: this timeout might be too large/small
            req = self.http_session.get( url, stream=True, 
                                        timeout=playlistinfo.target_duration)
            req.raise_for_status()

            if os.path.exists(path):
                print('File %s already exists, skipping' % (path))
                return

            with file( path, 'wb') as out:
                for chunk in req.iter_content(chunk_size):
                    out.write(chunk)
        except requests.HTTPError as ex:
            if ex.response.status_code == 404:
                self.defer_segment(seq, url, path)
            else:
                print('HTTP error fetching segment %s' % url)
                self.stats['failed'] += 1
            return
        except (ConnectionError, socket.timeout, requests.exceptions.Timeout) as ex:
            self.defer_segment(seq, url, path)
            return

    def defer_segment(self, seq, url, path):
        """
        Defer a segment download
        """
        self.retry_queue.put( (url, path))
        self.remove(path)
        self.stats['failed'] += 1
        print('Defering segment (%d) %s' % 
    		(seq, url))

    def print_info(self, seq, info):
        """
        Print worker status
        """
        print('#%d ' % seq, end='')
        for name, value in self.stats.items():
            print("%s: %s " % (name.capitalize(), value), end='')
        print("TargetDuration: %s" % info.target_duration, end='')
        print('')

def hls_playlist_loop(queue, playlist, http_session=requests.Session()):
    """
    Periodically refreshes a playlist and pushes the segment
    information into a queue

    * queue is the queue where we place segment information
      as (sequencenum, url, MediaInfo)
    * playlist URL
    * http_session is a requests.Session() object

    This function BLOCKS execution
    """
    queue = queue
    nextseq = -1

    failed = 0
    while True:
        try:
            strm = hls.get_stream(playlist, 
                                    http_session=http_session)
        except Exception as ex:
            failed += 1
            if failed < 3:
                continue
            else:
                print("Failed to get the playlist (%d) %s" % (failed, ex))
                return

        failed = 0
        if nextseq == -1 or strm.sequence >= nextseq:
            start = 0
        else:
            start = nextseq - strm.sequence

        for i in range(start, len(strm.segment_urls)):
            queue.put( (
                    strm.sequence+i,
                    strm.segment_urls[i],
                    strm.info
                ))
        nextseq = strm.sequence+len(strm.segment_urls)

        # TODO: if we start seeing the same playlist a lot ... (do something?)
        # according to the RFC we should scale the waittime
        if start == len(strm.segment_urls):
            print('Playlist is the same')

        # Sleep before refreshing the list
        time.sleep(strm.info.target_duration*0.5)


def main():
    """hlsdump <url> <path>"""
    if len(sys.argv) != 3:
        print('Usage: hlsdump <url> <path>')
        sys.exit(-1)
    url = sys.argv[1]
    path = sys.argv[2]

    queue = Queue.Queue()

    # HTTP session settings
    session = requests.Session()
    session.headers.update({'User-Agent': 'Apple-iPhone5C2/1001.405'})

    worker = HLSSegmentDownloader(queue, folder=path, http_session=session)
    worker.name_prefix = 'video-' + hashlib.md5(url).hexdigest()
    worker.start()

    # A cool trick to get a working video from these dumps
    #
    #     ls video*.ts | sort -t '-' -k 3n  | xargs cat > joinedfile
    #     ffmpeg -i joinedfile -c copy -bsf:a aac_adtstoasc final.mp4
    #

    try:
        print("Fetching HLS from %s" % url)
        hls_playlist_loop(queue, url, http_session=session)
        queue.join()
    except KeyboardInterrupt:
        print("Interrupted")
        sys.exit(-1)

if __name__ == '__main__':
    main()
