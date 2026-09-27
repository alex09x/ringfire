#!/usr/bin/env python3
"""Reject incomplete/lossy benchmark smoke logs (never judge performance thresholds)."""
import re
import sys
from pathlib import Path


def require(ok, context):
    if not ok:
        raise ValueError(context)


def summaries(text, expected, context):
    rows = re.findall(
        r"received (\d+) of (\d+) \(lost (\d+)\), duplicates (\d+), "
        r"out of order (\d+), out of range (\d+)", text
    )
    want = [(n, n, 0, 0, 0, 0) for n in expected]
    require([tuple(map(int, row)) for row in rows] == want, context + ': delivery counters')
    for key in ('lapped', 'gaps', 'echo_errors'):
        require(not re.search(rf'\b{key}[= ]+[1-9]\d*', text), context + ': ' + key)


def verify(root):
    def read(name):
        return (root / (name + '.log')).read_text()

    for transport in ('tcp', 'multicast'):
        name = 'latency-' + transport
        text = read(name)
        summaries(text, [1000, 10000], name)
        require(re.search(r'\bn=1000\b', text), name + ': sample count')
        name = 'ping-' + transport
        text = read(name)
        require(re.search(
            r'500 pings after 100 warm-up.*: 0 lost .*?, 0 late, 0 unexpected, reader lapped 0',
            text), name + ': measured reply counters')
        require(re.search(r'\brtt n=500\b', text), name + ': sample count')
    for transport in ('tcp', 'multicast', 'unicast'):
        name = 'stress-' + transport
        text = read(name)
        require(re.search(
            r'pushed 10000 .*echoed 10000 .*lost 0, duplicates 0, disorder 0, reader lapped 0',
            text), name + ': delivery counters')
        require(re.search(r'\brtt n=10000\b', text), name + ': sample count')
        summaries(read('master-' + transport), [1000, 1000], 'master-' + transport)
        summaries(read('slave-' + transport), [1000], 'slave-' + transport)
        require(len(re.findall(r'\bn=1000\b', read('master-' + transport))) == 2,
                'master-' + transport + ': sample count')
        require(re.search(r'\bn=1000\b', read('slave-' + transport)),
                'slave-' + transport + ': sample count')


if __name__ == '__main__':
    try:
        verify(Path(sys.argv[1]))
    except (OSError, ValueError) as error:
        sys.exit(f'benchmark smoke failed: {error}')
    print('All measured smoke phases delivered every expected record exactly once.')
