"""Μέτρηση wall-clock χρόνου ενός block. Πρέπει να περιβάλει action
(show/collect/write), αλλιώς δε μετρά τίποτα λόγω lazy evaluation."""
import time
from contextlib import contextmanager


@contextmanager
def timed(label):
    start = time.perf_counter()
    try:
        yield
    finally:
        print(f"[TIMER] {label}: {time.perf_counter() - start:.3f} s")
