import os
import subprocess


def set_current_process_to_highest_priority():
    subprocess.check_output(f"wmic process where processid={os.getpid()} CALL setpriority realtime")
