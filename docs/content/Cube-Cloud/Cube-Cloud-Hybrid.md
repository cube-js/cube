---
title: Cube Cloud Hybrid Installation
permalink: /cube-cloud-hybrid
category: Cube Cloud
menuOrder: 1
---

Cube Cloud can be installed as a hybrid cloud option.
In this case Cube Cloud acts as a Kubernetes operator.
In this case it doesn't have access to your VPC or database and manages your deployment only by means of Kubernetes API.
Below are instructions for popular cloud providers.

## AWS

First step of Cube Cloud installation is to create AWS EKS cluster:

1. Please see https://docs.aws.amazon.com/eks/latest/userguide/create-cluster.html on how to create cluster. 
If you choose to use AWS Management Console UI path please ensure you use same credentials to authorize in AWS Management Console and your AWS CLI. 
2. While creating cluster please ensure you use the same VPC and security groups that your database use.
3. Please refer to https://docs.aws.amazon.com/eks/latest/userguide/worker_node_IAM_role.html if you have problems with setting up worker node roles.
4. Install `kubectl` if you don't have one: https://kubernetes.io/docs/tasks/tools/install-kubectl/.
5. Setup `kubectl` using https://docs.aws.amazon.com/eks/latest/userguide/create-kubeconfig.html.
6. Go to Cube Cloud deployment Overview page and copy `kubectl apply -f ...` installation command.
7. Run just copied `kubectl apply -f ...` in your command line.
8. After all required Kubernetes services started up you should see your deployment status at the Cube Cloud deployment Overview page. 
