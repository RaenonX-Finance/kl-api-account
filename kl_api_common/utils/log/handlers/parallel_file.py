# Copied from https://stackoverflow.com/a/25387192/11571888
# Can"t use the original time rotated handler as it fails to rollover
import logging
import os
import re
import time
from logging.handlers import TimedRotatingFileHandler


class ParallelTimedRotatingFileHandler(TimedRotatingFileHandler):
    def __init__(
        self,
        filename: str,
        when="D",
        interval=1,
        backup_count=0,
        encoding=None,
        delay=False,
        utc=False,
        suffix=".log"
    ):
        self.origFileName = filename
        self.when = when.upper()
        self.interval = interval
        self.backupCount = backup_count
        self.utc = utc
        self.postfix = suffix

        if self.when == "S":
            self.interval = 1  # one second
            self.suffix = "%Y-%m-%d_%H-%M-%S"
            ext_match = r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$"
        elif self.when == "M":
            self.interval = 60  # one minute
            self.suffix = "%Y-%m-%d_%H-%M"
            ext_match = r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}$"
        elif self.when == "H":
            self.interval = 60 * 60  # one hour
            self.suffix = "%Y-%m-%d_%H"
            ext_match = r"^\d{4}-\d{2}-\d{2}_\d{2}$"
        elif self.when == "D" or self.when == "MIDNIGHT":
            self.interval = 60 * 60 * 24  # one day
            self.suffix = "%Y-%m-%d"
            ext_match = r"^\d{4}-\d{2}-\d{2}$"
        elif self.when.startswith("W"):
            self.interval = 60 * 60 * 24 * 7  # one week
            if len(self.when) != 2:
                raise ValueError(
                    "You must specify a day for weekly rollover from 0 to 6 (0 is Monday): %s" % self.when
                )
            if self.when[1] < "0" or self.when[1] > "6":
                raise ValueError("Invalid day specified for weekly rollover: %s" % self.when)
            self.dayOfWeek = int(self.when[1])
            self.suffix = "%Y-%m-%d"
            ext_match = r"^\d{4}-\d{2}-\d{2}$"
        else:
            raise ValueError("Invalid rollover interval specified: %s" % self.when)

        current_time = int(time.time())

        logging.handlers.BaseRotatingHandler.__init__(
            self,
            self.calc_file_name(current_time),
            "a",
            encoding,
            delay
        )

        self.extMatch = re.compile(ext_match)
        self.interval = self.interval * interval  # multiply by units requested

        self.rolloverAt = self.computeRollover(current_time)

    def computeRollover(self, current_time: int) -> int:
        return int(current_time - current_time % self.interval)

    def calc_file_name(self, current_time):
        if self.utc:
            time_tuple = time.gmtime(current_time)
        else:
            time_tuple = time.localtime(current_time)

        return self.origFileName + "." + time.strftime(self.suffix, time_tuple) + self.postfix

    # noinspection PyMethodOverriding
    def getFilesToDelete(self, name_file_new):
        name_dir_og, name_file = os.path.split(self.origFileName)
        name_dir_new, name_file_new = os.path.split(name_file_new)

        file_names = os.listdir(name_dir_og)
        result = []
        prefix = name_file + "."
        suffix = self.postfix
        pre_len = len(prefix)
        post_len = len(suffix)

        for file_name in file_names:
            if (
                    file_name[:pre_len] == prefix
                    and file_name[-post_len:] == suffix
                    and len(file_name) - post_len > pre_len
                    and file_name != name_file_new
            ):
                suffix = file_name[pre_len:len(file_name) - post_len]

                if self.extMatch.match(suffix):
                    result.append(os.path.join(name_dir_og, file_name))

        result.sort()

        if len(result) < self.backupCount:
            result = []
        else:
            result = result[:len(result) - self.backupCount]

        return result

    def doRollover(self):
        if self.stream:
            self.stream.close()
            # noinspection PyTypeChecker
            self.stream = None

        current_time = self.rolloverAt
        new_file_name = self.calc_file_name(current_time)
        new_base_file_name = os.path.abspath(new_file_name)
        self.baseFilename = new_base_file_name
        self.mode = "a"
        self.stream = self._open()

        if self.backupCount > 0:
            for s in self.getFilesToDelete(new_file_name):
                try:
                    os.remove(s)
                except OSError:
                    pass

        new_rollover_at = self.computeRollover(current_time)
        while new_rollover_at <= current_time:
            new_rollover_at = new_rollover_at + self.interval

        # If DST changes and midnight or weekly rollover, adjust for this.
        if (self.when == "MIDNIGHT" or self.when.startswith("W")) and not self.utc:
            dst_now = time.localtime(current_time)[-1]
            dst_at_rollover = time.localtime(new_rollover_at)[-1]

            if dst_now != dst_at_rollover:
                if not dst_now:  # DST kicks in before next rollover, so we need to deduct an hour
                    new_rollover_at = new_rollover_at - 3600
                else:  # DST bows out before next rollover, so we need to add an hour
                    new_rollover_at = new_rollover_at + 3600

        self.rolloverAt = new_rollover_at
