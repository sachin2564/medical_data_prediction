VERSION=v1
DOCKERUSER=nishamurarka1877

build:
        docker build -f Dockerfile-rest -t project-rest .

push:
        docker tag project-rest $(DOCKERUSER)/project-rest:$(VERSION)
        docker push $(DOCKERUSER)/sentiment-rest:$(VERSION)
        docker tag project-rest $(DOCKERUSER)/projectt-rest:latest
        docker push $(DOCKERUSER)/project-rest:latest

