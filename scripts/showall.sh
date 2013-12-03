#!/bin/bash

python -m scripts.show sysres "2048; 12; 10; [0.04, 0.08]; [0, 5, 10, 15, 20, 25, 30, 35, 40]"
python -m scripts.show execm "2048; 12; 10; [0, 5, 10, 15, 20, 25, 30]"
python -m scripts.show mres "2048; 12; 10; [0, 5, 10, 15, 20, 25, 30]"
python -m scripts.show spvsep "5; 30; 100; 50; 1000; [0.2, 0.4, 0.6, 0.8, 1.0]; [0.01, 0.04]"
python -m scripts.show spvsfp "5; 100; 50; 1000; [0.0]; [0.001, 0.0015, 0.002, 0.0025, 0.003]"
python -m scripts.show epelen "5; 100; 50; 1000; [0.2, 0.6]; [10, 30, 50, 70, 90]"
python -m scripts.show epsynch "5; 30; 100; 0.2; 50; 1000; [10, 30, 50, 70, 90]"
