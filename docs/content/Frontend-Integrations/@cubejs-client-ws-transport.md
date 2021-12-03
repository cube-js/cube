---
title: '@cubejs-client/ws-transport'
permalink: /@cubejs-client-ws-transport
category: Frontend Integrations
subCategory: Reference
menuOrder: 5
---

WebSocket transport for Cube.js client

## WebSocketTransport

### <--{"id" : "WebSocketTransport"}--> WebSocketTransport constructor

>  **new WebSocketTransport**(**__namedParameters**: object): *[WebSocketTransport](#web-socket-transport)*

### <--{"id" : "WebSocketTransport"}--> apiUrl

> **apiUrl**: *string*

### <--{"id" : "WebSocketTransport"}--> hearBeatInterval

> **hearBeatInterval**: *number*

### <--{"id" : "WebSocketTransport"}--> messageCounter

> **messageCounter**: *number*

### <--{"id" : "WebSocketTransport"}--> messageIdToSubscription

> **messageIdToSubscription**: *Record‹number, [Subscription](#types-subscription)›*

### <--{"id" : "WebSocketTransport"}--> messageQueue

> **messageQueue**: *[Message](#types-message)[]*

### <--{"id" : "WebSocketTransport"}--> token

> **token**: *string*

### <--{"id" : "WebSocketTransport"}--> ws

> **ws**: *any*

### <--{"id" : "WebSocketTransport"}--> authorization

### <--{"id" : "WebSocketTransport"}--> initSocket

>  **initSocket**(): *any*

### <--{"id" : "WebSocketTransport"}--> request

>  **request**(**method**: string, **__namedParameters**: object): *object*

### <--{"id" : "WebSocketTransport"}--> sendMessage

> `protected` **sendMessage**(**message**: any): *void*

## WebSocketTransportResult

### <--{"id" : "WebSocketTransportResult"}--> WebSocketTransportResult constructor

>  **new WebSocketTransportResult**(**__namedParameters**: object): *[WebSocketTransportResult](#result)*

### <--{"id" : "WebSocketTransportResult"}--> result

> **result**: *unknown*

### <--{"id" : "WebSocketTransportResult"}--> status

> **status**: *unknown*

### <--{"id" : "WebSocketTransportResult"}--> json

>  **json**(): *Promise‹unknown›*

## Types

### <--{"id" : "Types"}--> Message

Name | Type |
------ | ------ |
messageId | number |
method | string |
params | string |
requestId | any |

### <--{"id" : "Types"}--> Subscription

Name | Type |
------ | ------ |
callback |  (**result**: [WebSocketTransportResult](#result)) => *void* |
message | [Message](#types-message) |

### <--{"id" : "Types"}--> WebSocketTransportOptions

Name | Type |
------ | ------ |
apiUrl | string |
authorization | string |
hearBeatInterval? | number |
heartBeatInterval? | number |
