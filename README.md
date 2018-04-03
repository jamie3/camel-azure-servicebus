# Azure Service Bus Apache Camel Component

Camel Component for Azure Service Bus using Azure Service Bus Java API

# Code

Send message to topic

```java
from(...)
.to("azure-servicebus:topic:MyTopic")
```

Subscribe from topic

```java
from("azure-servicebus:topic:MyTopic?")
.to(...)
```

Subscribe to queue

```java
from("azure-servicebus:queue:MyQueue")
.to(...)
```

Send to queue

```java
from(...)
.to("azure-servicebus:queue:MyQueue")
```

# Additional Reading

https://github.com/Azure/azure-service-bus/tree/master/samples/Java/azure-servicebus