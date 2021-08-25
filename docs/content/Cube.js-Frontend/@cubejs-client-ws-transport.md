---
title: '@cubejs-client/ws-transport'
permalink: /@cubejs-client-ws-transport
category: Cube.js Frontend
subCategory: Reference
menuOrder: 5
---

WebSocket transport for Cube.js client

## WebSocketTransport

### <--{"name" : "WebSocketTransport"}--> WebSocketTransport constructor

>  **new WebSocketTransport**(**__namedParameters**: object): *[WebSocketTransport](#web-socket-transport)*

### <--{"name" : "WebSocketTransport"}--> apiUrl

> **apiUrl**: *string*

### <--{"name" : "WebSocketTransport"}--> hearBeatInterval

> **hearBeatInterval**: *number*

### <--{"name" : "WebSocketTransport"}--> messageCounter

> **messageCounter**: *number*

### <--{"name" : "WebSocketTransport"}--> messageIdToSubscription

> **messageIdToSubscription**: *Record‹number, [Subscription](#types-subscription)›*

### <--{"name" : "WebSocketTransport"}--> messageQueue

> **messageQueue**: *[Message](#types-message)[]*

### <--{"name" : "WebSocketTransport"}--> token

> **token**: *string*

### <--{"name" : "WebSocketTransport"}--> ws

> **ws**: *any*

### <--{"name" : "WebSocketTransport"}--> authorization

### <--{"name" : "WebSocketTransport"}--> initSocket

>  **initSocket**(): *any*

### <--{"name" : "WebSocketTransport"}--> request

>  **request**(**method**: string, **__namedParameters**: object): *object*

### <--{"name" : "WebSocketTransport"}--> sendMessage

> `protected` **sendMessage**(**message**: any): *void*

## WebSocketTransportResult

### <--{"name" : "WebSocketTransportResult"}--> WebSocketTransportResult constructor

>  **new WebSocketTransportResult**(**__namedParameters**: object): *[WebSocketTransportResult](#result)*

### <--{"name" : "WebSocketTransportResult"}--> result

> **result**: *unknown*

### <--{"name" : "WebSocketTransportResult"}--> status

> **status**: *unknown*

### <--{"name" : "WebSocketTransportResult"}--> json

>  **json**(): *Promise‹unknown›*

## Types

### <--{"name" : "Types"}--> Message

Name | Type |
------ | ------ |
messageId | number |
method | string |
params | string |
requestId | any |

### <--{"name" : "Types"}--> Subscription

Name | Type |
------ | ------ |
callback |  (**result**: [WebSocketTransportResult](#result)) => *void* |
message | [Message](#types-message) |

### <--{"name" : "Types"}--> WebSocketTransportOptions

Name | Type |
------ | ------ |
apiUrl | string |
authorization | string |
hearBeatInterval? | number |
heartBeatInterval? | number |
