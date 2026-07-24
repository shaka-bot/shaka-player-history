#!/usr/bin/env python3

# Shaka Player History Live Stream
#
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Monitor shaka-player-history encoder speed and other events."""

import argparse
import collections
import json
import re
import select
import systemd.journal


UNIT_NAME = 'shaka-player-history.service'

# Number of recent progress entries to measure the instantaneous speed over.
# A single step is unreliable: progress lines can reach journald in bursts,
# where several updates spanning many seconds of media land within a fraction
# of a wall-clock second.  A one-step rate then divides a large media delta by a
# tiny wall delta and explodes into the hundreds.  Measuring across the ends of
# a small window spans a wide enough wall-clock interval that such bursts wash
# out.
INSTANT_WINDOW = 5

# Minimum wall-clock span, in seconds, the window must cover before we trust a
# rate from it.  A window whose entries all landed within a fraction of a second
# is a burst longer than INSTANT_WINDOW (see above); its endpoints are as
# compressed as any single step, so it would spike just the same.  Below this
# floor we report no reading rather than a wild one.  Real progress entries here
# are seconds to minutes apart, so a healthy window clears this easily.
INSTANT_MIN_WALL_SPAN = 1.0


def main():
  parser = argparse.ArgumentParser(
      description=__doc__,
      formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('-n', '--number', type=int,
          default=100,
          help='How many recent logs to show (0 == all)')
  parser.add_argument('--follow', '-f', action='store_true', default=False,
          help='Show the most recent logs, then wait for new logs.')
  args = parser.parse_args()

  j = systemd.journal.Reader()
  j.add_match(_SYSTEMD_UNIT=UNIT_NAME)
  j.add_disjunction()
  j.add_match(UNIT=UNIT_NAME)

  # Tracks the instantaneous encoder speed across successive log entries.
  tracker = InstantSpeedTracker()

  if args.number == 0:
    # Show everything: start at the very beginning of the journal.
    j.seek_head()
  else:
    # Seek to |number| before the end, with respect to our filters.  This
    # happens iteratively so we can run our filters on each entry.
    j.seek_tail()
    seeked = 0
    while seeked <= args.number:
      raw_log = j.get_previous()
      if not raw_log:
        # We've reached the start of the journal; there are fewer than
        # |number| matching entries.  Without this, get_previous() keeps
        # returning an empty entry forever and the loop never ends.
        break
      if parse_log(raw_log) is not None:
        seeked += 1

  flush_logs(j, tracker)

  # If we're following, we use select.poll to wait for new events.
  if args.follow:
    p = select.poll()
    p.register(j, j.get_events())

    try:
      while True:
        p.poll()
        flush_logs(j, tracker)
    except KeyboardInterrupt:
      pass


def flush_logs(j, tracker):
  for raw_log in j:
    log = parse_log(raw_log)
    if log:
      # Compute the instantaneous speed from consecutive progress entries and
      # drop the raw media time, which was only needed for that calculation.
      if 'TIME' in log:
        log['INSTANT'] = tracker.update(log['TS'], log.pop('TIME'))
      log['TS'] = format_timestamp(log['TS'])
      print(log)


def format_timestamp(ts):
  return ts.isoformat()


def parse_log(log):
  if log.get('UNIT') == UNIT_NAME:
    # This is init logging about our service.
    parsed = parse_init_log(log)
  else:
    # This is a log from our service itself.
    parsed = parse_unit_log(log)

  # Could be None to indicate a log we're skipping.
  if parsed:
    return parsed


def parse_init_log(log):
  if 'JOB_TYPE' in log:
    return {
      'TS': log['__REALTIME_TIMESTAMP'],
      'JOB_TYPE': log['JOB_TYPE'],
      'MESSAGE': log['MESSAGE'],
    }
  return None


def parse_unit_log(log):
  parsed = {
    'TS': log['__REALTIME_TIMESTAMP'],
  }

  message = log['MESSAGE']
  # Using .* to match the last possible instance in the line
  match_speed = re.search(r'.*speed=\s*([0-9.]+)x', message)
  match_configs = re.search(r'Configs: (.*)', message)

  if match_speed:
    # This is ffmpeg's own speed field: a lifetime average of output produced
    # over wall clock elapsed since ffmpeg started.  It should sit at 1.0 for a
    # healthy live stream and can only drift slowly, so it hides brief stalls.
    parsed['SPEED'] = float(match_speed.group(1))

    # The output media position from the same progress line, in seconds.  We
    # difference this against the wall clock to derive an instantaneous speed
    # that reacts immediately to a stall.  Hours are unbounded (e.g. 631:12:31)
    # and can briefly be negative right after ffmpeg starts.
    match_time = re.search(
        r'time=\s*(-?)(\d+):(\d\d):(\d\d(?:\.\d+)?)', message)
    if match_time:
      sign = -1 if match_time.group(1) else 1
      hours = int(match_time.group(2))
      minutes = int(match_time.group(3))
      seconds = float(match_time.group(4))
      parsed['TIME'] = sign * (hours * 3600 + minutes * 60 + seconds)
  elif match_configs:
    parsed['CONFIGS'] = json.loads(match_configs.group(1))
  elif 'Fast-forward' in message:
    parsed['LOOP'] = True
    parsed['NEW_COMMITS'] = True
  elif 'Already up to date' in message:
    parsed['LOOP'] = True
    parsed['NEW_COMMITS'] = False
  else:
    return None

  return parsed


class InstantSpeedTracker(object):
  """Derives a near-instantaneous encoder speed from progress entries.

  ffmpeg's own speed field is a lifetime average, so once output stalls it
  decays only gradually and a total outage looks like a gentle decline.  We
  instead measure output media time produced per unit of journal wall clock
  over a small sliding window of recent entries.  This reacts within a few
  entries of a stall, drops to ~0 when output stops, and is robust to bursty
  journald delivery in a way a single-step rate is not (see INSTANT_WINDOW).
  """

  def __init__(self):
    # Recent (wall_ts, media_time) samples, oldest first, capped at
    # INSTANT_WINDOW entries by the deque itself.
    self._samples = collections.deque(maxlen=INSTANT_WINDOW)

  def update(self, ts, media_time):
    """Record one progress entry and return the windowed speed, or None.

    Returns None when there's no usable window yet: the first entry after
    startup or after a restart, where we have only a single sample to compare.
    """
    # A media time that jumps backwards means ffmpeg restarted and reset its
    # clock.  The samples still in the window are from the previous run, so
    # measuring across them would be meaningless; start the window over.
    if self._samples and media_time < self._samples[-1][1]:
      self._samples.clear()

    self._samples.append((ts, media_time))

    # Need at least two samples to measure a rate at all.
    if len(self._samples) < 2:
      return None

    old_ts, old_time = self._samples[0]
    wall_delta = (ts - old_ts).total_seconds()
    media_delta = media_time - old_time

    # The window collapsed into a burst: too little wall clock elapsed across it
    # to divide by.  No meaningful rate, so don't report a wild one.
    if wall_delta < INSTANT_MIN_WALL_SPAN:
      return None

    return round(media_delta / wall_delta, 3)


if __name__ == '__main__':
  main()
