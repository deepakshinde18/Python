def longestconsectiveseq():
    '''
    input -> [100, 4, 200, 1, 3, 2]
    output -> 4
    Explanation :- longest sequence in above input is [1,2,3,4]
    therefore its length is 4
    '''
    nums = [100, 4, 200, 1, 3, 2]
    nums_set = set(nums)

    max_seq_length = 0
    for i in range(len(nums)):
        current_num = nums[i]
        curr_seq_length = 1
        if (current_num - 1) not in nums_set:
            while ((current_num + 1) in nums_set):
                current_num += 1
                curr_seq_length += 1
            max_seq_length = max(max_seq_length, curr_seq_length)
    print(max_seq_length)

longestconsectiveseq()

