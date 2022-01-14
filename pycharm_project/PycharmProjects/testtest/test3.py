import speedtest
import socket

host = socket.gethostname()
print(host)
wifi = speedtest.Speedtest()
print(wifi.get_best_server())
print(wifi.get_servers())
download_speed = wifi.download()
upload_speed = wifi.upload()
ping_res = wifi.results.ping
print(f"Download speed is: {download_speed / 1024 / 1024:.2f}Mbps")
print(f"Upload speed is: {upload_speed / 1024 / 1024:.2f}Mbps")
print(f"ping--> {ping_res}ms")
