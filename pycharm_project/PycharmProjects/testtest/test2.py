# def power(a, b):
#     """Returns arg1 raised to power arg2."""
#
#     return a ** b
#
#
# print(power.__doc__)
# from numpy import random
# import seaborn as sns
# import matplotlib.pyplot as plt
#
# x = random.logistic(loc=2, scale=2, size=(2, 3))
# sns.distplot(x, hist=False)
#
# print(x)
# plt.show()
# import sys
# a = (1,2)
# print(sys.getsizeof(a))

# import random
# data = [10, 20, 30]
# random.shuffle(data)
# print(data)
# print(random.shuffle(data))
import socket

# import speedtest
# hostname = socket.gethostname()
# # dns = socket.
# wifi = speedtest.Speedtest()
# server = wifi.get_servers()
# print(wifi)
# print(server)
# print(wifi.get_best_server())
# import speedtest

try:
    address_ip = socket.gethostbyname("tsys.tss.net")

except:
    pass
finally:
    import speedtest

    host = socket.gethostname()
    print(host)
    wifi = speedtest.Speedtest()
    # print(wifi.get_best_server())
    print(wifi.get_servers())
    # wifi.set_mini_server("134.159.184.4")
    # download_speed = wifi.download()
    # upload_speed = wifi.upload()
    # ping_res = wifi.results.ping
    # print(f"Download speed is: {download_speed / 1024 / 1024:.2f}")
    # print(f"Upload speed is: {upload_speed / 1024 / 1024:.2f}")
    # print(ping_res)
    # print(wifi.results.share())

    import pyspeedtest
    #
    # test = pyspeedtest.SpeedTest("www.google.com")
    # print(test.ping())
    # print(test.download() / 1024 / 1024)
    # test = pyspeedtest.SpeedTest("ispan-eu.tsys.com")
    # print(test.ping())
    # test = pyspeedtest.SpeedTest("ispan-na.tsys.com")
    # print(test.ping())


    # print(wifi.results.share())
    # print(wifi.get_config())
    # test = pyspeedtest.SpeedTest("ispan-eu.tsys.com")
    # test = pyspeedtest.SpeedTest("www.google.com")
    # print(test.ping())
    # print(str(test.download()))
    # print(str(test.upload()))


# print(ip)
# ipaddr = socket.gethostbyname(hostname)
# print(f"hostname:{hostname}")
# print(f"address{ipaddr}")
