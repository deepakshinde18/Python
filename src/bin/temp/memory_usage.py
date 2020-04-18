import memory_profiler
import random
import time

names = ['Kiran', 'King', 'John', 'Corey']
majors = ['Math', 'Comps', 'Science']

print('Memory (Before): {}Mb'.format(memory_profiler.memory_usage()))


def people_list(num_people):
    results = []
    for i in range(num_people):
        person = {
                    'id': i,
                    'name': random.choice(names),
                    'major': random.choice(majors)
                  }
        results.append(person)
    return results


def people_generator(num_people):
    for i in range(num_people):
        person = {
                    'id': i,
                    'name': random.choice(names),
                    'major': random.choice(majors)
                  }
        yield person

t1 = time.clock()
people = people_list(10000000)
t2 = time.clock()


# t1 = time.clock()
# people = people_generator(10000000)
# t2 = time.clock()

print('Memory (After): {}Mb'.format(memory_profiler.memory_usage()))
print('Took {} Seconds to run this'.format(t2-t1))
