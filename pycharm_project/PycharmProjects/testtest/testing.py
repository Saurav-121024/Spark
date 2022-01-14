# # a = 1
# # b = 1
# # print(id(a))
# # print(id(b))
# # list_a = [1, 2, 3]
# # print(id(list_a[0]))
# # string intern()
# # integer intern()
#
# # a = "s" * 4096
# # b = "s" * 4096
# # print(id(a), id(b))
# # b = "d" * 1023
# # print(id(b))
# # list_a = [1, 2, 3, ['a', 'b', 'c'], (1, 2, 3)]
# # print(len(list_a))
# # import copy
# # a = [1, 2, 3]
# # b = a.copy()
# # b[1] = 5
#
# list_a = [1, 2, 3, 4, 1,]
# # a = {"egg", "yolk", "fruit"}
# # # print(list_a[0])
# # # print(a[0])
# # print("search found" if 'eg' in a else "search ele not found")
# # for x,y in enumerate(list_a):
# #     print(x,y)
# # for x in list_a:
# #     print(list_a.index(x), x)
# list_a.pop()
# print(list_a)
#
#
# import matplotlib.pyplot as plt
# import seaborn as sns
#
# sns.distplot([0, 1, 4, 5], hist=False)
#
# plt.show()
import copy
# a = [[1,2,3],[4,5,6]]
# # c = []
#
# b = a.copy()
# # c = copy.deepcopy(a)
# a[0][1] = 5
# c[0] = 10
# print(id(b[0]), b[0])
# print(id(a[0]), a[0])
# print(id(c[0]), c[0])
# for x in range(len(a)):
#     c.append(a[x])

# a[0] = 5
#
# for x in zip(a, c):
#     print(x)

# b = (1,2,3,4,1,"abc", True)
# c = {"apple", "cidar"}
# d = {'a':1, 'b':2,}
# print(b)
# print(id(a[0]))
# print(id(b), id(c))
# print(a)
# print(c,d)

# ____________________________________________________________________________________________________
import copy

# a = [1, [1,2,3], ["abc","cde"]]
# b = iter(a)
# print(next(b))
# print(next(b))
# print(next(b))
a = [1, 2, 3, 4, 5]

c = [i ** 2 for i in a]


# def func(x):
#     return x ** 2


# b = list(map(lambda x: x** 2, a))
# print(b)
# print(c)
#
# # for i in range(5):
#

#


# b = a.copy()
# a[0] = 5
# a[1] = tuple(a[1])
# # a[1][1] = 5
#
# print(b)


# from numpy import random
# import matplotlib.pyplot as plt
# import seaborn as sns
#
# sns.distplot(random.binomial(n=10, p=0.5, size=1000), hist=True, kde=False)
#
# plt.show()

def func1(a):

    """jsclkjcLKjclk
    ydfhgkjhkjhkjhkjhkjhfhgfgfdgfdgfdgfdgfgfd"""
    a = 10  # enclosed
    # globals()['b'] =10
    b = 15
    print(a) #10
    # for x in globals():
    #     print(f"global var{x}")

    def func2(c):
        """hello test"""
        nonlocal a
        a = 15 #local
        # print(a + ) #15
        b = 20
        print(f"a{a}")

    print(a) #10
    print(func2.__doc__)

    func2(3)
    print(a) #10
    print(f"b{a}")

a = 5  # global
# print(a) # 5

print(func1.__doc__)

func1(a)
# b = 16
# print(a) #5

# global and globals()[]
