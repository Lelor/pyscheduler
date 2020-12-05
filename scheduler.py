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
from json import load
class Task:
    def __init__(self, _id, description, due_date, estimated_time):
        self._id = _id
        self.description = description
        self.due_date = due_date
        self.estimated_time = estimated_time

    def run(self):
        # Task execution rules would be applied here
        print(f'Running task of id <{self._id}>, estimated time: {self.estimated_time} hours')
        return '<status: success>'
        
class ExecutionGroup:
    def __init__(self, *tasks):
        self.tasks = list(tasks)

    def __repr__(self):
        return f'{[t._id for t in self.tasks]}'
    
    def run_all(self):
        return list(map(lambda t: {t._id: t.run()}))
    
    def append_task(self, task):
        self.tasks.append(task)

    @property
    def task_ids(self):
        return [t._id for t in self.tasks]

    @property
    def estimated_execution_time(self):
        return sum(map(lambda t: t.estimated_time, self.tasks))

class Scheduler:
    def __init__(self, start_datetime, end_datetime):
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self._execution_groups = []
        self._tasks = None

    def serialize_tasks(self, obj):
        self._tasks = list(map(lambda task: Task(**task), obj))

    def serialize_execution_groups(self, timeframe=timedelta(hours=8)):
        remaining = self._tasks.copy()
        for idx, task in enumerate(self._tasks):
            # saves a little bit of processing on the last iteration, not really necessary
            if len(remaining) == 1:
                self._execution_groups.append(ExecutionGroup(*remaining))
                break
            # Iterating over every task twice to compare the sets
            # TODO: Implement logic to take the timeframe limit in consideration
            if task._id in [r._id for r in remaining]:
                group = ExecutionGroup(task)
                for comparable_task in self._tasks[idx+1:]:
                    if group.estimated_execution_time + comparable_task.estimated_time <= 8:
                        group.append_task(comparable_task)
                        # reconstructs the remaining set of tasks, may refactor it later
                        remaining = [i for i in remaining if not (i._id in map(lambda x: x._id, group.tasks))]
                self._execution_groups.append(group)

    def load_jobs(self, obj):
        self.serialize_tasks(obj)
        self.serialize_execution_groups()


if __name__ == '__main__':
    sch = Scheduler(1, 2)
    with open('test.json', 'r') as f:
        example_obj = load(f)
    sch.load_jobs(example_obj)
    print(sch._execution_groups)