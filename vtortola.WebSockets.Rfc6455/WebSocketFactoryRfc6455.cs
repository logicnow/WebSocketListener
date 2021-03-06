﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Net;

namespace vtortola.WebSockets.Rfc6455
{
    public class WebSocketFactoryRfc6455 : WebSocketFactory
    {
        public override Int16 Version { get { return 13; } }
        public WebSocketFactoryRfc6455()
            :base()
	    {
	    }
        public WebSocketFactoryRfc6455(WebSocketListener listener)
            :base(listener)
        {
        }
        public Action<WebSocket> OnPingReceived { get; set; }

        public override WebSocket CreateWebSocket(Stream stream, WebSocketListenerOptions options, IPEndPoint localEndpoint, IPEndPoint remoteEndpoint, WebSocketHttpRequest httpRequest, WebSocketHttpResponse httpResponse, List<IWebSocketMessageExtensionContext> negotiatedExtensions)
        {
            var webSocket = new WebSocketRfc6455(stream, options, localEndpoint, remoteEndpoint, httpRequest, httpResponse, negotiatedExtensions);
            if (OnPingReceived != null)
                webSocket.OnPingReceived = OnPingReceived;
            return webSocket;
        }
    }
}
