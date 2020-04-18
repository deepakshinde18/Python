def rev_list():
    '''
    input = ['a', 'b', 'c', 'd', 'e']
    output = ['e', 'd', 'c', 'b', 'a']
    '''
    input = ['a', 'b', 'c', 'd', 'e']
    left = 0
    right = len(input) - 1
    while left <= right:
        temp = input[left]
        input[left] = input[right]
        input[right] = temp
        left += 1
        right -= 1
    print('resultant reverse output list is :- {}'.format(input))

rev_list()

