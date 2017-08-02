import pickle
from Player import Player

items = ['axe', 'sword', 'guns']

myObj = Player(1, 'Deepak', 100.00, items)
print(myObj)

with open('classobj.pkl', 'wb') as outfile:
    pickle.dump(myObj, outfile, pickle.HIGHEST_PROTOCOL)

print('=========================')
newObj = None

with open('classobj.pkl', 'rb') as infile:
    newObj = pickle.load(infile)

print(newObj)
