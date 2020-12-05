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

class Scheduler:
    def __init__(self, start_datetime, end_datetime):
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self._execution_groups = None

    def serialize_exc_groups(self, obj):
        self.execution_groups = map(lambda task: ExecutionGroup(**task), obj)


class ExecutionGroup:
    pass