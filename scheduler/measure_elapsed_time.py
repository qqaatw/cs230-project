from datetime import datetime


class Elapsed_Time_Tracker:
    def __init__(self):
        self.start_time = None
        self.end_time = None

    def set_start_time(self):
        if self.start_time is None:
            self.start_time = datetime.now()

    def update_end_time(self):
        self.end_time = datetime.now()

    def measure_elapsed_time(self):
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        else:
            return None
