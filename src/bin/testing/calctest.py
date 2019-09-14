import unittest
import calc


class TestClass(unittest.TestCase):

    def test_add(self):
        result = calc.add(4, 5)
        self.assertEqual(result, 9)


if __name__ == '__main__':
    unittest.main()