from collections import Sequence

import numpy as np


# all operations are O(1) and don't require copying the array
# except to_array which has to copy the array and is O(n)
class CircularQueue:

    def __init__(self, obj: Sequence, maxlen: int):
        # allocate the memory we need ahead of time
        self.max_length: int = maxlen
        self.queue_tail: int = maxlen - 1
        self.queue_head: int = 0
        o_len = len(obj)
        if o_len == maxlen:
            self.rec_queue = np.array(obj)
        elif o_len > maxlen:
            self.rec_queue = np.array(obj[o_len - maxlen:])
        else:
            self.rec_queue = np.append(np.array(obj), np.zeros(maxlen - o_len))
            self.queue_tail = o_len - 1

    def to_array(self) -> np.array:
        if self.queue_tail < self.queue_head:
            return self.rec_queue[self.queue_head:] + self[:self.queue_head]
        return self.rec_queue[:self.queue_tail]
        # return np.roll(self.rec_queue, -self.queue_head)  # this will force a copy

    def enqueue(self, new_data) -> None:
        # move tail pointer forward then insert at the tail of the queue
        # to enforce max length of recording
        self.queue_tail = (self.queue_tail + 1) % self.max_length
        self.rec_queue[self.queue_tail] = new_data
        if self.queue_tail == self.queue_head:
            self.queue_head = (self.queue_tail + 1) % self.max_length

    def peek(self) -> int:
        return self.rec_queue[self.queue_head]

    def __setitem__(self, index: int, new_value: int):
        loc = (self.queue_tail + 1 + index) % self.max_length
        self.rec_queue[loc] = new_value

    def __getitem__(self, key) -> int:
        if isinstance(key, slice):

            if key.start is not None and key.start != 0:
                raise ValueError("Not implemented yet :/ slice start must be 0")
            if key.step is not None and key.step != 1:
                raise ValueError("Not implemented yet :/ slice step must be 1")
            if key.stop is None:
                key.stop = self.max_length - 1
            start = self.queue_head
            stop = self.queue_head + key.stop
            if stop < start:
                start = (self.queue_head + self.max_length + key.stop) % self.max_length
                stop = (start - key.stop) % self.max_length
                if stop == 0:
                    stop = self.max_length

            if start < stop:
                return self.rec_queue[start:stop]
            else:
                return np.concatenate((self.rec_queue[start:],
                                       self.rec_queue[:stop]), axis=None)

        # the item we want will be at head + index
        loc = (self.queue_head + key) % self.max_length
        return self.rec_queue[loc]

    def __repr__(self):
        return str(self)

    def __str__(self):
        return f"tail: {self.queue_tail}\narray: {self.rec_queue}"
