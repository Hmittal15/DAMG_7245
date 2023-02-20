from fastapi.testclient import TestClient
from main import app

client=TestClient(app)

def test_hello():
    response=client.get("/hello")
    assert response.status_code==200
    mess=response.json()["message"]
    assert mess=="Hello world!"

def test_name():
    response=client.post(url= "/user_name", json={"n": "Harshit"})
    assert response.status_code==200
    testName = response.json()['name']
    assert testName=="Harshit"
