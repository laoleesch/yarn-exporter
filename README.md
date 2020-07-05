# yarn-exporter
YARN API prometheus exporter

![Travis (.org)](https://img.shields.io/travis/laoleesch/yarn-exporter?style=flat-square) ![Docker Cloud Build Status](https://img.shields.io/docker/cloud/build/laoleesch/yarn-exporter?style=flat-square)

Inspired by https://github.com/PBWebMedia/yarn-prometheus-exporter
Additionaly:
- [x] queues metrics (/ws/v1/cluster/scheduler)
- [ ] running apps metrics (/ws/v1/cluster/apps?state=RUNNING)
- [ ] basic auth
