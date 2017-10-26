"""
HBase rest接口
"""
import requests

baseurl = "http://myd6:20550"


response = requests.get(baseurl+'/hy_userinfo/12299990010,90000010', headers={"Accept" : "application/json"})

print(response.json())
