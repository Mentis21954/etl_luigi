import luigi
from datetime import datetime

class HelloWorld(luigi.Task):
    # no upstream requirements at all
    def requires(self):
        return None

    # creates a local file as output
    def output(self):
        return luigi.LocalTarget('helloworld.txt')

    # the actual job to perform
    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('Hello World!\n')


if __name__ == '__main__':
    start_time = datetime.now()
    luigi.run(main_task_cls=HelloWorld, local_scheduler= True)
    end_time = datetime.now()
    print('Duration: {}'.format(end_time - start_time))
