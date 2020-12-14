#
#  ------------------------------------------------------
#  |  SCHEDULER               - start_datetime          |
#  |                          - end_datetime            |
#  |                          - execution_groups        |
#  |                          - serialize_tasks         |
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
    
    def __repr__(self):
        return f'Task<{self._id}, {self.description}>'

    def run(self):
        # Task execution rules would be applied here
        print(f'Running task of id <{self._id}>, estimated time: {self.estimated_time} hours')
        return '<status: success>'
        
class ExecutionGroup:
    def __init__(self, start_datetime, *tasks):
        self.start_datetime = start_datetime
        self.tasks = list(tasks)

    def __repr__(self):
        return f'{[t._id for t in self.tasks]}'
    
    def run_all(self):
        res = map(lambda t: {t._id: t.run()}, self.tasks)
        return {k: v for r in res for k, v in r.items()}
    
    def append_task(self, task):
        self.tasks.append(task)

    @property
    def task_ids(self):
        return map(lambda t: t._id, self.tasks)

    @property
    def estimated_execution_time(self):
        return sum(map(lambda t: t.estimated_time, self.tasks))

    @property
    def estimated_end_time(self):
        return self.start_datetime + timedelta(hours=self.estimated_execution_time)


class Scheduler:
    def __init__(self, start_datetime, end_datetime):
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.time_leftover = self.end_datetime - self.start_datetime
        self._execution_groups = []
        self._tasks = None

    def serialize_tasks(self, obj):
        self._tasks = list(map(lambda task: Task(**task), obj))
        # orders the tasks by the 'due_date" asc
        self._tasks.sort(key=lambda i: i.due_date)

    def serialize_execution_groups(self, timeframe=timedelta(hours=8)):
        remaining_tasks = self._tasks.copy()
        for idx, task in enumerate(self._tasks):
            # Saves a little bit of processing on the last iteration, not really necessary
            if len(remaining_tasks) == 1:
                group = ExecutionGroup(self.end_datetime - self.time_leftover, *remaining_tasks)
                self._execution_groups.append(group)
                self.time_leftover -= timedelta(hours=group.estimated_execution_time)
                break
            # Iterating over every task twice to compare the sets
            # TODO: Implement logic to take the timeframe limit in consideration
            if task._id in [r._id for r in remaining_tasks]:
                group = ExecutionGroup(self.end_datetime - self.time_leftover, task)
                for comparable_task in self._tasks[idx+1:]:
                    if group.estimated_execution_time + comparable_task.estimated_time <= 8:
                        group.append_task(comparable_task)
                        # reconstructs the remaining set of tasks, may refactor it later
                        remaining_tasks = [i for i in remaining_tasks if not (i._id in group.task_ids)]
                self._execution_groups.append(group)
                self.time_leftover -= timedelta(hours=group.estimated_execution_time)

    # Serializes the jobs json into usable data for the scheduler
    def load_jobs(self, obj):
        self.serialize_tasks(obj)
        self.serialize_execution_groups()


if __name__ == '__main__':
    dt_start = datetime.fromisoformat('2019-11-10 09:00:00')
    dt_end = datetime.fromisoformat('2019-11-11 12:00:00')
    sch = Scheduler(dt_start, dt_end)

    with open('example.json', 'r') as f:
        example_obj = load(f)
    sch.load_jobs(example_obj)
    print(sch._execution_groups)
    # print(f'{f"overdue {sch.time_leftover}" if sch.time_leftover < timedelta(hours=0) else f"{sch.time_leftover} leftover"}')
    print(sch.time_leftover)