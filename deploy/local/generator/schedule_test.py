import unittest

from schedule import run_for


class Clock:
    def __init__(self):
        self.t = 0.0
        self.slept = []

    def now(self):
        return self.t

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.t += seconds


class ScheduleTest(unittest.TestCase):
    def test_five_minutes_every_twenty_seconds(self):
        clock = Clock()
        count = run_for(300, 20, clock.now, clock.sleep, lambda: None)
        self.assertEqual(count, 15)
        self.assertEqual(clock.t, 300)
        self.assertEqual(clock.slept, [20] * 15)

    def test_does_not_sleep_past_the_deadline(self):
        clock = Clock()
        run_for(25, 20, clock.now, clock.sleep, lambda: None)
        self.assertEqual(clock.slept, [20, 5])
        self.assertEqual(clock.t, 25)

    def test_zero_duration(self):
        self.assertEqual(run_for(0, 20, lambda: 0, lambda _seconds: None, lambda: None), 0)


if __name__ == "__main__":
    unittest.main()
