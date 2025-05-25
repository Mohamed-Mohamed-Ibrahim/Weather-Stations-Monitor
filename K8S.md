# Installation

1. Docker
2. [Minikube](https://minikube.sigs.k8s.io/docs/start/?arch=%2Flinux%2Fx86-64%2Fstable%2Fbinary+download)
3. [kubectl](https://kubernetes.io/docs/tasks/tools/)

# Commands To Run Kafka

1. Using docker without `sudo` 
   ```
   $ sudo groupadd docker
   $ sudo usermod -aG docker $USER
   $ newgrp docker
   ```
2. starting minikube `minikube start --driver=docker`
3. Running all `kubectl apply -f ./k8s/` -> Do not this till the end as some services are fully done
   1. Running kafka `kubectl apply -f ./k8s/kafka/`
      1. Entering kafka `kubectl exec --stdin --tty <pod> -- /bin/sh` -> all staff will be found at `/opt/bitnami/kafka/bin/`
      2. To communicate with kafka few examples are in Resources
   2. Running central_station `kubectl apply -f ./k8s/central_station/`
      1. if for the first time => should build Dockerfile `docker build -t base-central-station:latest Backend/`
      2. then add the image name in the k8s yaml file 
      3. or running using docker `docker run -d --name base-central-station -p 8080:8080 -v ./data:/app/data base-central-station:latest`
   3. Running weather_station `kubectl apply -f ./k8s/weather_station/`
      1. if for the first time => should build Dockerfile `docker build -t weather_station weather_station/`
      2. then add the image name in the k8s yaml file
   4. Running elastic_search & kibana ``
      1. upload to Elasticsearch
         1. for the first time `docker build -t upload-el:latest Client/`
         2. otherwise `docker run -d --name base-central-station -v ./data:/app/data base-central-station:latest`
4. 

# Useful commands

- K8s
1. `kubectl get all`
2. `kubectl get <object>`
3. `kubectl delete all --all`
4. `kubectl delete -f your-config.yaml` -> clean up resources
5. `kubectl delete pv shared-data-pv` -> clean up Retain pv
6. `kubectl diff -f k8s/central_station/central_station.yaml` -> diff from before applying 
7. `kubectl describe <object>` -> like logs in docker
8. `kubectl port-forward pod/base-central-station-6ff5d6df88-pf99w 8080:8080` -> expose <object>

- Minikube
8. `minikube image load base-central-station:latest` -> load local image in minikube

- Docker
1. `docker system prune -a -f` -> remove everything

# Resources

1. [freecodecamp](https://www.freecodecamp.org/news/the-kubernetes-handbook/#heading-installing-kubernetes)
2. Dockerfile in k8s
   1. [video](https://youtu.be/3mdCiFu52XA)
   2. [article](https://medium.com/@haider.mtech2011/introduction-to-using-dockerfiles-in-a-kubernetes-setup-for-950661b36a8b)
   3. [stackoverflow](https://stackoverflow.com/questions/35061746/run-jar-file-in-docker-image)
3. [kafka](https://www.geeksforgeeks.org/setup-kafka-on-kubernetes/)
4. [Elastic Search](https://medium.com/@ismailwajdi39/deploying-elasticsearch-and-kibana-on-kubernetes-with-password-protection-fad93010563c)