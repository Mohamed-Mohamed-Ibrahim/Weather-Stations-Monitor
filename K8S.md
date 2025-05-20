# Installation

1. Docker
2. [Minikube](https://minikube.sigs.k8s.io/docs/start/?arch=%2Flinux%2Fx86-64%2Fstable%2Fbinary+download)
3. [kubectl](https://kubernetes.io/docs/tasks/tools/)

# Commands To Run Kafka

1. Add 
   ```
   $ sudo groupadd docker
   $ sudo usermod -aG docker $USER
   $ newgrp docker
   ```
2. starting minikube `minikube start --driver=docker`

# Resources

1. [freecodecamp](https://www.freecodecamp.org/news/the-kubernetes-handbook/#heading-installing-kubernetes)