import heapq

# Weight of the model size
W_size = 1
# Weight of the number of iteration
W_iteration = 1


class Task:
    def __init__(
        self, task_id, username, python_command, size, iteration, inference, age=0
    ):
        self.task_id = task_id
        self.username = username
        self.python_command = python_command
        self.size = size
        self.iteration = iteration
        self.inference = inference
        self.age = age

    def __lt__(self, other):
        if self.age == other.age:
            return (W_size * self.size + W_iteration * self.iteration) < (
                W_size * other.size + W_iteration * other.iteration
            )
        return self.age > other.age


class PriorityQueue:
    def __init__(self):
        self.queue = []

    def push(self, task):
        heapq.heappush(self.queue, task)

    def pop(self):
        return heapq.heappop(self.queue)

    def empty(self):
        return len(self.queue) == 0

    def size(self):
        return len(self.queue)

    def age_tasks(self):
        for i in range(len(self.queue)):
            self.queue[i].age += 1
