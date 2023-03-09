import luigi


class TaskA(luigi.Task):
    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('task_a')

    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('foo')


class TaskB(luigi.Task):
    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('task_b')

    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('bar')


class TaskC(luigi.Task):
    def requires(self):
        return TaskA()  # <-- Notice this dependency!

    def output(self):
        return luigi.LocalTarget(self.input().path + '.task_c')

    def run(self):
        with self.input().open() as infile, self.output().open('w') as outfile:
            for line in infile:
                outfile.write(line)


# Let's create an own "copy" of TaskC, that depends on TaskB instead of TaskA:

class MyTaskC(TaskC):
    def requires(self):
        return TaskB()  # <-- Notice how we switched the dependency in TaskC!


if __name__ == '__main__':
    luigi.run()
