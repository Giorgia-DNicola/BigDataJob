import time
from datetime import datetime


class Timer:
    def __init__(self):
        self.start_time = None
        self.elapsed_time = 0
        self.running = False

    def start(self):
        """Start the timer."""
        if not self.running:
            self.start_time = time.time() - self.elapsed_time
            self.running = True
        else:
            print("Timer is already running.")

    def stop(self):
        """Stop the timer and calculate the elapsed time."""
        if self.running:
            self.elapsed_time = time.time() - self.start_time
            self.running = False
        else:
            print("Timer is not running.")

    def reset(self):
        """Reset the timer."""
        self.start_time = None
        self.elapsed_time = 0
        self.running = False

    def elapsed(self):
        """Get the elapsed time since the timer was started."""
        if self.running:
            return time.time() - self.start_time
        return self.elapsed_time

    def __str__(self):
        """Get a human-readable string of the elapsed time."""
        elapsed = self.elapsed()
        hours, rem = divmod(elapsed, 3600)
        minutes, seconds = divmod(rem, 60)
        return "{:0>2}:{:0>2}:{:05.2f} ".format(int(hours), int(minutes), seconds) + ",\n started at " + str(datetime.now())

    def print_current_timestamp(self):
        return str(datetime.now())
