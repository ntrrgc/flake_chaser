#!/usr/bin/env python3
import sys
import time
import random

print(repr(sys.argv), flush=True)
while True:
    time.sleep(0.1)
    n = random.random()
    if n > 0.99:
        print("Test failed.", flush=True)
        time.sleep(1000000)
    elif n > 0.90:
        print("Test succeeded.", flush=True)
        time.sleep(1000000)
    else:
        print("Blah blah blah", flush=True)