from metaflow import FlowSpec
import metaflow.api as ma
from metaflow.api import foreach, step, join


class OldJoinFlow1(FlowSpec):
    @step
    def start(self):
        self.next(self.generate_ints)

    @step
    def generate_ints(self):
        self.ints = list(range(1, 16))
        self.next(self.test_prime, foreach="ints")

    @step
    def test_prime(self):
        n = self.input
        self.is_prime = n >= 2
        i = 2
        while i * i <= n:
            if n % i == 0:
                self.is_prime = False
                break
            i += 1
        self.next(self.fizzbuzz)

    @step
    def fizzbuzz(self):
        n = self.input
        self.n = n
        if n % 15 == 0:
            self.fb = "fizzbuzz"
        elif n % 3 == 0:
            self.fb = "fizz"
        elif n % 5 == 0:
            self.fb = "buzz"
        self.next(self.join)

    @step
    def join(self, branches):
        self.results = [
            {
                "n": branch.n,
                "is_prime": branch.is_prime,
                **({"fizzbuzz": branch.fb} if hasattr(branch, "fb") else {}),
            }
            for branch in branches
        ]
        self.next(self.end)

    @step
    def end(self):
        pass


class NewJoinFlow1(ma.FlowSpec):
    @step
    def generate_ints(self):
        self.ints = list(range(1, 16))

    @foreach("ints")
    def test_prime(self, n):
        self.n = n
        self.is_prime = n >= 2
        i = 2
        while i * i <= n:
            if n % i == 0:
                self.is_prime = False
                break
            i += 1

    @step
    def fizzbuzz(self):
        n = self.input
        if n % 15 == 0:
            self.fb = "fizzbuzz"
        elif n % 3 == 0:
            self.fb = "fizz"
        elif n % 5 == 0:
            self.fb = "buzz"

    @join("fizzbuzz")
    def join(self, branches):
        self.results = [
            {
                "n": branch.n,
                "is_prime": branch.is_prime,
                **({"fizzbuzz": branch.fb} if hasattr(branch, "fb") else {}),
            }
            for branch in branches
        ]


class OldJoinFlow2(FlowSpec):
    @step
    def start(self):
        self.next(self.generate_ints)

    @step
    def generate_ints(self):
        self.ints = list(range(1, 16))
        self.next(self.test_prime, foreach="ints")

    @step
    def test_prime(self):
        n = self.input
        self.is_prime = n >= 2
        i = 2
        while i * i <= n:
            if n % i == 0:
                self.is_prime = False
                break
            i += 1
        self.next(self.fizzbuzz)

    @step
    def fizzbuzz(self):
        n = self.input
        self.n = n
        if n % 15 == 0:
            self.fb = "fizzbuzz"
        elif n % 3 == 0:
            self.fb = "fizz"
        elif n % 5 == 0:
            self.fb = "buzz"
        self.next(self.join)

    @step
    def join(self, branches):
        self.results = [
            {
                "n": branch.n,
                "is_prime": branch.is_prime,
                **({"fizzbuzz": branch.fb} if hasattr(branch, "fb") else {}),
            }
            for branch in branches
        ]
        self.next(self.filter_odds)

    @step
    def filter_odds(self):
        self.odds = [r for r in self.results if r["n"] % 2 == 1]
        self.next(self.end)

    @step
    def end(self):
        pass


class NewJoinFlow2(ma.FlowSpec):
    @step
    def generate_ints(self):
        self.ints = list(range(1, 16))

    @foreach("ints")
    def test_prime(self):
        n = self.input
        self.n = n
        self.is_prime = n >= 2
        i = 2
        while i * i <= n:
            if n % i == 0:
                self.is_prime = False
                break
            i += 1

    @step
    def fizzbuzz(self):
        n = self.input
        if n % 15 == 0:
            self.fb = "fizzbuzz"
        elif n % 3 == 0:
            self.fb = "fizz"
        elif n % 5 == 0:
            self.fb = "buzz"

    @join("fizzbuzz")
    def join(self, branches):
        self.results = [
            {
                "n": branch.n,
                "is_prime": branch.is_prime,
                **({"fizzbuzz": branch.fb} if hasattr(branch, "fb") else {}),
            }
            for branch in branches
        ]

    @step
    def filter_odds(self):
        self.odds = [r for r in self.results if r["n"] % 2 == 1]


class OldForeachSplit(FlowSpec):
    @step
    def start(self):
        self.items = [1, 2, 3, 4]
        self.next(self.foreach, foreach="items")

    @step
    def foreach(self):
        n = self.input
        self.n = n
        self.n2 = n * n
        self.next(self.f1, self.f2)

    @step
    def f1(self):
        self.n3 = self.n * self.n2
        self.next(self.f3)

    @step
    def f2(self):
        self.n4 = self.n2 * self.n2
        self.next(self.f3)

    @step
    def f3(self, inputs):
        assert not hasattr(self, "n2")
        assert not hasattr(self, "n3")
        assert not hasattr(self, "n4")
        self.merge_artifacts(inputs)
        n = self.n
        assert (n, self.n2, self.n3, self.n4) == (n, n**2, n**3, n**4)
        self.n5 = self.n2 * self.n3
        self.next(self.join_foreach)

    @step
    def join_foreach(self, inputs):
        assert not hasattr(self, "items")
        assert not hasattr(self, "n")
        assert not hasattr(self, "n2")
        assert not hasattr(self, "n3")
        assert not hasattr(self, "n4")
        self.s = sum(input.n for input in inputs)
        self.s2 = sum(input.n2 for input in inputs)
        self.s3 = sum(input.n3 for input in inputs)
        self.s4 = sum(input.n4 for input in inputs)
        self.s5 = sum(input.n5 for input in inputs)
        self.next(self.end)

    @step
    def end(self):
        assert not hasattr(self, "items")
        assert (self.s, self.s2, self.s3, self.s4, self.s5,) == (
            10,
            30,
            100,
            354,
            1300,
        )


class NewForeachSplit(ma.FlowSpec):
    @step
    def start(self):
        self.items = [1, 2, 3, 4]

    @foreach("items")
    def foreach(self, n):
        self.n = n
        self.n2 = n * n

    @step("foreach")
    def f1(self):
        self.n3 = self.n * self.n2

    @step("foreach")
    def f2(self):
        self.n4 = self.n2 * self.n2

    @join("f1", "f2")
    def f3(self, inputs):
        assert not hasattr(self, "n2")
        assert not hasattr(self, "n3")
        assert not hasattr(self, "n4")
        self.merge_artifacts(inputs)
        n = self.n
        assert (n, self.n2, self.n3, self.n4) == (n, n**2, n**3, n**4)
        self.n5 = self.n2 * self.n3

    @join
    def join_foreach(self, inputs):
        assert not hasattr(self, "items")
        assert not hasattr(self, "n")
        assert not hasattr(self, "n2")
        assert not hasattr(self, "n3")
        assert not hasattr(self, "n4")
        self.s = sum(input.n for input in inputs)
        self.s2 = sum(input.n2 for input in inputs)
        self.s3 = sum(input.n3 for input in inputs)
        self.s4 = sum(input.n4 for input in inputs)
        self.s5 = sum(input.n5 for input in inputs)

    @step
    def end(self):
        assert not hasattr(self, "items")
        assert (self.s, self.s2, self.s3, self.s4, self.s5,) == (
            10,
            30,
            100,
            354,
            1300,
        )
