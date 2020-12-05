#
#  ------------------------------------------------------
#  |  SCHEDULER               - start_datetime          |
#  |                          - end_datetime            |
#  |                          - execution_groups        |
#  |                          - serialize_exc_groups    |
#  |   *Execution groups*                               |
#  | ------------------------  ------------------------ |
#  | | group1    --------   |  | group2    --------   | |
#  | | --------  | Task |   |  | --------  | Task |   | |
#  | | | Task |  --------   |  | | Task |  --------   | |
#  | | --------             |  | --------             | |
#  | ------------------------  ------------------------ |
#  ------------------------------------------------------
# Scheduler constraints:
#  - Sum of estimated elapsed time for all tasks in the execution group must be <= 8 hours
#  - Cannot leave boundaries of the inputted time limit

from datetime import datetime, timedelta
from itertools import combinations


class Scheduler:
    def __init__(self, start_datetime, end_datetime):
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self._execution_groups = None
        self._tasks = None

    def serialize_tasks(self, obj):
        self._tasks = map(lambda task: Task(**task), obj)

    def group_tasks_by_timeframe(self, timeframe=timedelta(hours=8)):
        pass


class Task:
    def __init__(self, _id, description, due_date, estimated_time):
        self._id = _id
        self.description = description
        self.due_date = due_date
        self.estimated_time = estimated_time
    def run(self):
        print(f'Running task of id <{self._id}>, estimated time: {self.estimated_time} hours')
        return '<status: success>'
        
class ExecutionGroup:
    def __init__(self, tasks):
        self.tasks = tasks
    
    def run_all(self):
        return list(map(lambda t: {t._id: t.run()}))