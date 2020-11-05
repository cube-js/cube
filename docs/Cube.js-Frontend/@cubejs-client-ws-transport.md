---
title: '@cubejs-client/ws-transport'
permalink: /@cubejs-client-ws-transport
category: Cube.js Frontend
subCategory: Reference
menuOrder: 5
---

WebSocket transport for Cube.js client

## WebSocketTransport

### constructor

>  **new WebSocketTransport**(**__namedParameters**: object): *[WebSocketTransport](#web-socket-transport)*

### apiUrl

> **apiUrl**: *string*

### hearBeatInterval

> **hearBeatInterval**: *number*

### messageCounter

> **messageCounter**: *number*

### messageIdToSubscription

> **messageIdToSubscription**: *Record‹number, [Subscription](#types-subscription)›*

### messageQueue

> **messageQueue**: *[Message](#types-message)[]*

### token

> **token**: *string*

### ws

> **ws**: *any*

### authorization

### initSocket

>  **initSocket**(): *any*

### request

>  **request**(**method**: string, **__namedParameters**: object): *object*

### sendMessage

> `protected` **sendMessage**(**message**: any): *void*

## WebSocketTransportResult

### constructor

>  **new WebSocketTransportResult**(**__namedParameters**: object): *[WebSocketTransportResult](#web-socket-transport-result)*

### result

> **result**: *unknown*

### status

> **status**: *unknown*

### json

>  **json**(): *Promise‹unknown›*

## Types

### Message

Name | Type |
------ | ------ |
messageId | number |
method | string |
params | string |
requestId | any |

### Subscription

Name | Type |
------ | ------ |
callback |  (**result**: [WebSocketTransportResult](#web-socket-transport-result)) => *void* |
message | [Message](#types-message) |

### WebSocketTransportOptions

Name | Type |
------ | ------ |
apiUrl | string |
authorization | string |
hearBeatInterval? | number |
heartBeatInterval? | number |
