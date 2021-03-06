import sys
import logging
import time
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler, FileSystemEventHandler


class OnMyWatch:
    # Set the directory on watch
    watchDirectory = "responses"

    def __init__(self):
        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.watchDirectory, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            print("Observer Stopped")

        self.observer.join()


class Handler(FileSystemEventHandler):
    @staticmethod
    def on_any_event(event):
        if event.is_directory:
            return None

        elif event.event_type == 'created':
            # Event is created, you can process it now
            print("Watchdog received created event - % s." % event.src_path)
        elif event.event_type == 'modified':
            # Event is modified, you can process it now
            print("Watchdog received modified event - % s." % event.src_path)

if __name__ == "__main__":

    watch = OnMyWatch()
    watch.run()

    # logging.basicConfig(level=logging.INFO,
    #                     format='%(asctime)s - %(message)s',
    #                     datefmt='%Y-%m-%d %H:%M:%S')
    # #path = sys.argv[1] if len(sys.argv) > 1 else '.'
    # path = 'responses'
    # event_handler = LoggingEventHandler()
    # observer = Observer()
    # observer.schedule(event_handler, path, recursive=True)
    # observer.start()
    # try:
    #     while observer.isAlive():
    #         observer.join(1)
    # except KeyboardInterrupt:
    #     observer.stop()
    # observer.join()