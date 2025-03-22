# Define the name of the Docker image to use and the container name
DOCKER_IMAGE=python:3.12
CONTAINER_NAME=fx-1-minute-data

# Target to create and run the container (in detached mode)
create-container:
	docker run -d --name $(CONTAINER_NAME) -v .:/app -w /app $(DOCKER_IMAGE) tail -f /dev/null
	
start-container:
	docker start $(CONTAINER_NAME)

stop-container:
	docker stop $(CONTAINER_NAME)

remove-container:
	docker rm $(CONTAINER_NAME)

install-requirements:
	docker exec $(CONTAINER_NAME) pip install -r /app/requirements.txt

download-all-raw:
	docker exec $(CONTAINER_NAME) python download_all_fx_data.py

download-all-delta:
	docker exec -it $(CONTAINER_NAME) python download_all_delta.py

update-delta:
	docker exec $(CONTAINER_NAME) python update_delta.py