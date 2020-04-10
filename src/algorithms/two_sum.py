def two_sum(nums, target):
    """
    Given nums = [2, 7, 11, 15], target = 9,
    Because nums[0] + nums[1] = 2 + 7 = 9,
    return [0, 1]
    """
    num_map = {}
    for i, num in enumerate(nums):
        if (target - nums[i]) in num_map:
            return [num_map[target - nums[i]], i]
        num_map[num] = i
    return -1


print(two_sum([2, 11, 15, 12], 9))

