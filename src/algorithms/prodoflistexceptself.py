def prodoflistexceptself():
    '''
    input -> [1, 2, 3, 4]
    output -> [24, 12, 8, 6]
    '''
    input = [1, 2, 3, 4]
    n = len(input)
    result = [None] * n

    result[0] = 1
    for i in range(1, n):
        result[i] = input[i-1] * result[i-1]

    R = 1
    for i in range(n-1, -1, -1):
        result[i] = result[i] * R
        R = R * input[i]
    print(result)

prodoflistexceptself()

