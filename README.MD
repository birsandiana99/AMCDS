Set up CommunicationProtocol file
``` 
protoc communication-protocol.proto --java_out=ABSOLUTE_PATH_PROTOBUF_FOLDER
```

Start up the hub with three processes
```
dalgs  127.0.0.1  5000  127.0.0.1  5001 5002 5003
```