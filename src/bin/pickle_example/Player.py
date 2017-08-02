class Player(object):

    def __init__(self, ID, name, health, items):
        self.ID = ID
        self.name = name
        self.health = health
        self.items = items

    def __str__(self):
        return 'My ID: {} \nMy name: {}\nMy Health: {}\nMy Items: {}.'.format(
            self.ID, self.name, self.health, self.items)

