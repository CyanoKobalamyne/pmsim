import unittest
from unittest import TestCase

import puppetmaster as pm


class TestSimple(TestCase):
    def test_01(self):
        """Single transaction without objects."""
        sched = pm.Scheduler()
        m = pm.Machine(1, sched)
        tr = pm.Transaction(set(), set(), 42)
        time = m.run({tr})
        self.assertEqual(42, time)


if __name__ == "__main__":
    unittest.main()
