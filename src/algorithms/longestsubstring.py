def longestsubstring():
    '''
    input -> 'abcabcbb'
    output -> 3
    explanation : longestsubstring is 'abc' and its length is 3
    '''
    s = 'abcabcbb'
    hash_set = set()
    i = 0
    j = 0
    maxlen = 0
    while j < len(s):
        if s[j] not in hash_set:
            hash_set.add(s[j])
            j += 1
            maxlen = max(maxlen, len(hash_set))
        else:
            hash_set.remove(s[i])
            i += 1
    print(maxlen)


longestsubstring()


