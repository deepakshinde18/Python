def fib(n):
    if n <=2:
        result = 1
    else:
        result = fib(n-2) + fib(n-1)
    return result
print(fib(1000))
