import pickle

dict1 = dict(a=100, b=200, c=300)
list1 = [400, 500, 600]

print(dict1)
print(list1)

outfile = open('pickle.pkl', 'wb')
pickle.dump(dict1, outfile, pickle.HIGHEST_PROTOCOL)
pickle.dump(list1, outfile, pickle.HIGHEST_PROTOCOL)

outfile.close()
print('===================================')

inputfile = open('pickle.pkl', 'rb')
dict2 = pickle.load(inputfile)
list2 = pickle.load(inputfile)

print(dict2)
print(list2)

