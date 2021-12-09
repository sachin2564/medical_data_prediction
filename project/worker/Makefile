VERSION=v1
DOCKERUSER=nishamurarka1877

build:
        docker build -f Docker-worker -t project-worker .

push:
        docker tag project-worker $(DOCKERUSER)/project-worker:$(VERSION)
        docker push $(DOCKERUSER)/project-worker:$(VERSION)
