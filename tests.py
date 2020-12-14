from datetime import datetime, timedelta
from unittest import TestCase
from json import load

import scheduler


class TestSchedulerJsonLoader(TestCase):
    def setUp(self):
        self.default_interval = (datetime.fromisoformat('2019-11-10 09:00:00'),
                                 datetime.fromisoformat('2019-11-11 12:00:00'))
        with open('example.json', 'r') as f:
            self.default_jobs = load(f)

    def test_task_should_run_successfully(self):
        t1 = scheduler.Task(1, 'xpto job', self.default_interval[1], 5)
        self.assertEqual(t1.run(), '<status: success>')

    def test_should_be_able_to_append_task_to_execution_group(self):
        eg = scheduler.ExecutionGroup(self.default_interval[0])
        t1 = scheduler.Task(1, 'xpto job', self.default_interval[1], 5)
        t2 = scheduler.Task(2, 'xpto job', self.default_interval[1], 5)
        eg.append_task(t2)
        eg.append_task(t1)
        self.assertEqual(list(eg.task_ids), [2, 1])

    def test_should_isolate_all_jobs_with_8_hours_plus(self):
        for job in self.default_jobs:
            job['estimated_time'] = 20
        sch = scheduler.Scheduler(*self.default_interval)
        sch.load_jobs(self.default_jobs)
        result = list(map(lambda eg: list(eg.task_ids), sch._execution_groups))
        self.assertEqual(result, [[1], [3], [2]])
        self.assertEqual(sch.time_leftover, timedelta(days=-2, seconds=54000))

    def test_should_load_execution_groups_with_max_timespan_of_8_hours(self):
        sch = scheduler.Scheduler(*self.default_interval)
        sch.load_jobs(self.default_jobs)
        result = list(map(lambda eg: list(eg.task_ids), sch._execution_groups))
        self.assertEqual(result, [[1, 3], [2]])
        self.assertEqual(sch.time_leftover, timedelta(hours=12))

    def test_should_load_tasks_sorted_by_deadline_asc(self):
        sch = scheduler.Scheduler(*self.default_interval)
        sch.load_jobs(self.default_jobs)
        result = list(map(lambda t: t._id, sch._tasks))
        self.assertEqual(result, [1, 3, 2])

    def test_should_run_all_execution_groups_in_order(self):
        sch = scheduler.Scheduler(*self.default_interval)
        sch.load_jobs(self.default_jobs)
        result = [eg.run_all() for eg in sch._execution_groups]
        expected_result = [{1: '<status: success>', 3: '<status: success>'}, {2: '<status: success>'}]
        self.assertEqual(result, expected_result)

