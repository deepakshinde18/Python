# class A(object):
#     def __new__(cls):
#         print("Creating instance")
#         return super(A, cls).__new__(cls)
#
#     def __init__(self):
#         print("Init is called")
#
# A()


# class Employee(object):
#     def __init__(self, name, salary):
#         self.name = name
#         self.salary = salary
#
#     def __new__(cls, name, salary):
#         if 0 < salary < 10000:
#             return object.__new__(cls)
#             return super(Employee, cls).__new__(cls)
#         else:
#             return None
#
#     def __str__(self):
#         return '{0}({1})'.format(self.__class__.__name__, self.__dict__)
#
# emp_tom = Employee.__new__(Employee, 'Tom', 8000)
# emp_richard = Employee('Richard', 20000)



# class GeeksforGeeks(object):
#     def __init__(self, firstname, lastname):
#         self.firstname = firstname
#         self.lastname = lastname
#
#     def __str__(self):
#         return "GeeksforGeeks"
#
#
# class Geek(object):
#     def __new__(cls):
#         return GeeksforGeeks('Deepak', 'Shinde')
#
#     def __init__(self):
#         print("Inside init")
#
#     def add(self):
#         print('Deepak Shinde')
#
#
# a = Geek()
# print(a)
# print(a.firstname)

class B:
    def __init__(self, first, last):
        self.first = first
        self.last = last

    def __new__(cls, *args, **kwargs):
        print('In class B')
        return super().__new__(cls)


class A(B):
    def __init__(self, name, lastname):
        self.firstname = name
        self.lastname = lastname
        super().__init__(name, lastname)

    def __new__(cls, *args, **kwargs):
        print('creating instance of the class A')
        return super().__new__(cls)

    def add(self):
        return 4 + 5

# a = A('Deepak', 'Shinde')
# print(a.first, a.last)

a = A.__new__(A)
a.__init__('Deepak', 'Shinde')
print(a.first, a.last)


# a = A.__new__(A)
# a.__init__('Deepak', 'Shinde')
# print(a.add())