# call by reference example

# def foo(bar):
#     bar.append(42)
#     print('value of bar in function foo :- {}'.format(bar))
#
# answer_list = []
# foo(answer_list)
# print('answer_list is :- {}'.format(answer_list))

# def foo(bar=[]):
#     bar.append(42)
#     print('value of bar in function foo :- {}'.format(bar))
#
# answer_list = [1, 2, 3, 4]
# foo(answer_list)
# print('answer_list is :- {}'.format(answer_list))


# call by refernce example

# def foo(bar):
#     print('before assignment value of bar is :- {}'.format(bar))
#     bar = 'new value'
#     print('value of bar in function foo :- {}'.format(bar))
#
# answer_list = 'old value'
# foo(answer_list)
# print('answer_list is :- {}'.format(answer_list))


def foo(bar):
    print('before assignment value of bar is :- {}'.format(bar))
    bar = (6, 7, 8, 9, 10)
    print('value of bar in function foo :- {}'.format(bar))

answer_list = (1, 2, 3, 4, 5)
foo(answer_list)
print('answer_list is :- {}'.format(answer_list))

