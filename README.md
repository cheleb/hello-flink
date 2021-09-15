
minikube tunnel


./bin/kubernetes-session.sh -Dkubernetes.cluster-id=my-first-flink-cluster -Dkubernetes.rest-service.exposed.type=ClusterIp



./bin/flink run\                                                                                                          
    --target kubernetes-session \
    -Dkubernetes.cluster-id=my-first-flink-cluster \
    ./examples/streaming/TopSpeedWindowing.jar

/bin/flink cancel \                                                                                                       
    --target kubernetes-session \
    -Dkubernetes.cluster-id=my-first-flink-cluster 58bf605dcd54716b662abac2a453eb61

./bin/flink list --target kubernetes-application -Dkubernetes.cluster-id=my-first-application-cluster -Dkubernetes.rest-service.exposed.type=ClusterIp


kubectl delete my-first-flink-cluster



eval $(minikube docker-env) 

```
./bin/flink run-application \
    --target kubernetes-application \
    -Dkubernetes.cluster-id=my-first-application-cluster \
    -Dkubernetes.container.image=hello-flink \
    -Dkubernetes.rest-service.exposed.type=ClusterIp \
    local:///opt/flink/usrlib/root-assembly-0.1.0-SNAPSHOT.jar
```

kubectl get pod -o=name

```
kubectl port-forward my-first-application-cluster-58cbb6489c-vk2jd 8888:8081
```

