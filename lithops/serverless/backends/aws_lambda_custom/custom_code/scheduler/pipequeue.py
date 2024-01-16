from torch import multiprocessing
import queue
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
import pickle

class FlexQueue:
    def __init__(self, mode = "Queue", num_pipes: int= None):
        self.mode = mode
        self.num_pipes = num_pipes
        if mode=="InputPipeQueue":
            self.queue = InputPipeQueue(num_pipes)
        elif mode=="OutputPipeQueue":
            self.queue = OutputPipeQueue(num_pipes)
        else:
            self.queue = TheadingQueue()
        self.closed = False

    def reopen(self):
        if self.mode=="InputPipeQueue":
            self.queue = InputPipeQueue(self.num_pipes)
        elif self.mode=="OutputPipeQueue":
            self.queue = OutputPipeQueue(self.num_pipes)
        else:
            self.queue = queue.Queue()
        self.closed = False
    def put(self, item, id:int=None):
        if self.mode=="InputPipeQueue":
            self.queue.put(item)
        elif self.mode == "OutputPipeQueue":
            self.queue.put(item, id=id)
        else:
            self.queue.put(item)

    def get(self, id:int=None):
        if self.mode=="InputPipeQueue":
            return self.queue.get(id=id)
        elif self.mode == "OutputPipeQueue":
            return self.queue.get(id=id)
        else:
            return self.queue.get()

    def isQueue(self):
        if self.mode=="Queue":
            return True
        else:
            return False

    def isInputPipeQueue(self):
        if self.mode=="InputPipeQueue":
            return True
        else:
            return False

    def isOutputPipeQueue(self):
        if self.mode == "OutputPipeQueue":
            return True
        else:
            return False

    def get_nowait(self, id:int=None):
        if self.mode=="InputPipeQueue":
            return self.queue.get_nowait(id=id)
        elif self.mode == "OutputPipeQueue":
            return self.queue.get_nowait(id=id)
        else:
            return self.queue.get_nowait()

    def get_any(self):
        if self.mode=="InputPipeQueue":
            while True:
                for id in range(self.num_pipes):
                    item = self.queue.get_nowait(id=id)
                    if item is not None:
                        return item
        elif self.mode == "OutputPipeQueue":
            while True:
                for id in range(self.num_pipes):
                    item = self.queue.get_nowait(id=id)
                    if item is not None:
                        return item
        else:
            return self.queue.get()

    def get_any_nowait(self):
        if self.mode=="InputPipeQueue":
            for id in range(self.num_pipes):
                item = self.queue.get_nowait(id=id)
                if item is not None:
                    return item
        elif self.mode == "OutputPipeQueue":
            for id in range(self.num_pipes):
                item = self.queue.get_nowait(id=id)
                if item is not None:
                    return item
        else:
            return self.queue.get_nowait()

    def empty(self):
        return self.queue.empty()

    def startInputPipeQueue(self, nones_to_finish: int):
        self.queue.start(nones_to_finish)

    def close(self):
        self.closed=True
        self.queue.close()


class TheadingQueue:
    def __init__(self):
        self.queue = queue.Queue()
        self.counter = 0
        self.counter_lock = threading.Lock()

    def put(self, item):
        self.queue.put(item)

    def get(self):
        return self.queue.get()

    def get_nowait(self):
        return self.queue.get_nowait()

    def empty(self):
        return self.queue.empty()

    def close(self):
        pass




class OutputPipeQueue:
    def __init__(self, num_pipes:int=1, main_process: bool = True ):
        self.pipes = [multiprocessing.Pipe(False) for _ in range(num_pipes)]
        if main_process:
            self.queue_main = queue.Queue()

    def put(self, item, id:int=None):
        # Enqueue item to pipe with id=id. If id is None, enqueue item to first available pipe.
        if id != None:
            if id >= 0:
                pipe = self.pipes[id]
                pipe[1].send(item)
            else:
                self.queue_main.put(item)
        else:
            while True:
                for i, pipe in enumerate(self.pipes):
                    if not pipe[0].poll():
                        pipe[1].send(item)
                        return


    def get(self, id:int=0):
        if id >= 0:
            while True:
                pipe = self.pipes[id]
                if pipe[0].poll():
                    item = pipe[0].recv()
                    return item
        else:
            return self.queue_main.get()


    def get_any(self):
        try:
            while True:
                for id in range(len(self.pipes)):
                    item = self.get_nowait(id=id)
                    if item is not None:
                        return item
                try:
                    item=self.queue_main.get_nowait()
                except Exception as e:
                    pass
                if item is not None:
                    return item
        except Exception as e:
            print(f"Get Any - An error occurred: {e}")


    def get_nowait(self, id:int=0):
        try:
            pipe = self.pipes[id]
            if pipe[0].poll():
                item = pipe[0].recv()
                return item
            else:
                return None
        except Exception as e:
            print(f"Get Nowait - An error occurred: {e}")

    def empty(self):
        #Check if pipes are closed
        for pipe in self.pipes:
            try:
                if pipe[0].poll():
                    return False
            except EOFError:
                pass
        return True

    def close(self):
        for pipe in self.pipes:
            pipe[0].close()
            pipe[1].close()


class InputPipeQueue:
    def __init__(self, num_pipes: int = 1, main_process: bool = True):
        self.pipes = [multiprocessing.Pipe(False) for _ in range(num_pipes)]
        self.main_process = main_process
        self.num_pipes = num_pipes
        self.num_threads = num_pipes
        if num_pipes > 0:
            self.threadpool = ThreadPoolExecutor(max_workers=self.num_threads)
        self.threads = []
        self.continue_threads = True
        self.continue_threads_lock = threading.Lock()
        self.input_queue = queue.Queue()
        self.counter = 0
        self.counter_lock = threading.Lock()
        self.start()

    def start(self):
        try:
            for i in range(self.num_pipes):
                self.threads.append(self.threadpool.submit(self.run_thread, i))
        except Exception as e:
            print(f"start - An error occurred: {e}")

    def stop(self):
        try:
            with self.continue_threads_lock:
                self.continue_threads = False
            for i in range(self.num_threads + 1):
                self.input_queue.put(None)
        except Exception as e:
            print(f"stop - An error occurred: {e}")

    def run_thread(self, id: int):
        try:
            while True:
                item = self.input_queue.get()
                self.send(item, id)

                if item is None:
                    with self.continue_threads_lock:
                        if not self.continue_threads:
                            break

        except Exception as e:
            print(f"run thread - An error occurred: {e}")

    def put(self, item):
        try:
            self.input_queue.put(item)
        except Exception as e:
            print(f"put- An error occurred: {e}")

    def send(self, item, id: int = 0):
        try:
            pipe = self.pipes[id]
            pipe[1].send(item)
        except Exception as e:
            print(f"send - An error occurred: {e}")

    def get(self, id: int = -1):
        try:
            if id >= 0:
                while True:
                    pipe = self.pipes[id]
                    if pipe[0].poll():
                        item = pipe[0].recv()
                        return item
            else:
                item = self.input_queue.get()
                return item
        except Exception as e:
            print(f"get - An error occurred: {e}")

    def empty(self):
        #Check if pipes are closed
        try:
            for pipe in self.pipes:
                if pipe[0].poll():
                    return False
        except EOFError:
            return True
        return True

    def close(self):
        self.stop()
        for pipe in self.pipes:
            pipe[0].close()
            pipe[1].close()
        for thread in self.threads:
            thread.cancel()


