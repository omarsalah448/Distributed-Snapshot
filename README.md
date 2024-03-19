# Distributed-Snapshot

This project implements the Chandy-Lamport algorithm for distributed snapshots, where there are multiple nodes communicating with each other and running multiple snapshots at the same time.
The goal is to capture the system's state without missing or capturing any duplicated tokens.

- To run a specific testcase 'Test2NodesSingleMessage' for ex; you can use the command "go test -run Test2NodesSingleMessage  -v -count=1" as shown in the picture
![image](https://github.com/omarsalah448/Distributed-Snapshot/assets/108231831/ec032bfd-1200-4006-a9b5-2e751eb0198e)

- To run all the test cases at one you can use the command "go test -run . -v -count=1" as shown in the picture
![image](https://github.com/omarsalah448/Distributed-Snapshot/assets/108231831/1e1ec9fe-0b27-43ec-8f0e-e9390ade1304)
