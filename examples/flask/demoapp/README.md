# Example Flask project using Aqueduct
The service receives an image and returns the result of image classification.


## Run and test

Cd to directory
```shell
cd examples/flask/demoapp
```

Run python with installed requirements from `requirements.txt`.
```shell
#  for MacOs add 
# OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES 
FLASK_APP=demoapp.app flask run
```
To send request you can use `curl` utility
```shell
curl localhost:5000/ --data-binary @data/apple.jpg
```