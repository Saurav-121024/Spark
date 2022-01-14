import socket

try:
    address_ip = socket.gethostbyname("tsys.tss.net")

except:
    pass
else:
    import speedtest

    host = socket.gethostname()
    print(f"system name:{host}")
    wifi = speedtest.Speedtest()
    print(wifi.get_best_server())
    download_speed = wifi.download()
    upload_speed = wifi.upload()
    ping_res = wifi.results.ping
    print(f"Download speed is: {download_speed / 1024 / 1024:.2f}Mbps")
    print(f"Upload speed is: {upload_speed / 1024 / 1024:.2f}Mbps")
    print(f"{ping_res}ms")
    print(wifi.results.share())

    import pyspeedtest

    test = pyspeedtest.SpeedTest("www.google.com")
    print(f"pinging to google: {test.ping()}ms")
    print(f"downloading speed wrt google: {test.download() / 1024 / 1024}Mbps")
    try:
        test = pyspeedtest.SpeedTest("ispan-eu.tsys.com")
    except:
        pass
    else:
        print(f"pinging to European ispan: {test.ping()}ms")
    try:
        test = pyspeedtest.SpeedTest("ispan-na.tsys.com")
    except:
        pass
    else:
        print(f"pinging to North America ispan: {test.ping()}ms")
