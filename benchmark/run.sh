#!/bin/bash

time python benchmark.py --timer=wallclock result-wallclock.json
time python2 barplot.py result-wallclock.json result-wallclock.png

time python benchmark.py --timer=cpu result-cpu.json
time python2 barplot.py result-cpu.json result-cpu.png
