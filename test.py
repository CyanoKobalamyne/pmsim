import unittest
from unittest import TestCase

import puppetmaster as pm


class TestSimple(TestCase):
    def test_01(self):
        sched = pm.Scheduler()
        m = pm.Machine(1, sched)
        tr = pm.Transaction(set(), set(), 1)
        time = m.run({tr})
        self.assertEqual(1, time)


if __name__ == "__main__":
    unittest.main()
