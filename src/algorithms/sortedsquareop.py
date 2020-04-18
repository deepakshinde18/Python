def sqop():
    """
    input -> [-7, -3, -1, 4, 8, 12]
    output -> [1, 9, 16, 49, 64, 144]
    """
    input = [-7, -3, -1, 4, 8, 12]
    n = len(input)
    result = [None] * n
    left = 0
    right = n - 1
    print(right)
    for i in range(n-1, -1, -1):
        if abs(input[left]) > abs(input[right]):
            result[i] = input[left] * input[left]
            left += 1
        else:
            result[i] = input[right] * input[right]
            right -= 1
    print(result)

sqop()

