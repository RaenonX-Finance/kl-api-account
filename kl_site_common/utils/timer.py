import time


class ExecTimer:
    def __init__(self, name: str):
        self.start_sec = time.perf_counter()
        self.name = name

    @property
    def exec_time_sec(self) -> float:
        return time.perf_counter() - self.start_sec

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print(f"Timer [{self.name}]: {self.exec_time_sec:.3f} s")
