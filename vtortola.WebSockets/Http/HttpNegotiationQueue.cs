using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace vtortola.WebSockets.Http
{
    public sealed class HttpNegotiationQueue : IDisposable
    {
        readonly BufferBlock<Socket> _sockets;
        readonly BufferBlock<WebSocketNegotiationResult> _negotiations;
        readonly CancellationTokenSource _cancel;
        readonly WebSocketHandshaker _handShaker;
        readonly WebSocketListenerOptions _options;
        readonly WebSocketConnectionExtensionCollection _extensions;
        private Action _startNextRequestAsync;
        private int _currentOutstandingAccepts;
        private int _currentOutstandingRequests;

        public HttpNegotiationQueue(WebSocketFactoryCollection standards, WebSocketConnectionExtensionCollection extensions, WebSocketListenerOptions options)
        {
            _options = options;
            _extensions = extensions;
            _cancel = new CancellationTokenSource();
            _startNextRequestAsync = new Action(ProcessRequestsAsync);

            _sockets = new BufferBlock<Socket>(new DataflowBlockOptions() {
                BoundedCapacity = options.NegotiationQueueCapacity,
                CancellationToken = _cancel.Token
            });
            _negotiations = new BufferBlock<WebSocketNegotiationResult>(new DataflowBlockOptions() {
                BoundedCapacity = options.NegotiationQueueCapacity,
                CancellationToken = _cancel.Token,
            });

            _cancel.Token.Register(_sockets.Complete);
            _cancel.Token.Register(_negotiations.Complete);

            _handShaker = new WebSocketHandshaker(standards, _options);

            OffloadStartNextRequest();
        }

        private bool CanAcceptMoreRequests
        {
            get
            {
                return (_currentOutstandingAccepts < _options.ParallelNegotiations
                    && _currentOutstandingRequests < _options.NegotiationQueueCapacity - _currentOutstandingAccepts);
            }
        }

        private void OffloadStartNextRequest()
        {
            if (!_cancel.IsCancellationRequested && CanAcceptMoreRequests)
            {
                Task.Factory.StartNew(_startNextRequestAsync);
            }
        }


        private async void ProcessRequestsAsync()
        {
            while (!_cancel.IsCancellationRequested && CanAcceptMoreRequests)
            {
                Interlocked.Increment(ref _currentOutstandingAccepts);

                Socket socket = null;
                try
                {
                    socket = await _sockets.ReceiveAsync(_cancel.Token).ConfigureAwait(false);
                }
                catch (TaskCanceledException)
                {
                }
                catch (Exception)
                {
                    Interlocked.Decrement(ref _currentOutstandingAccepts);
                    return;
                }

                Interlocked.Decrement(ref _currentOutstandingAccepts);
                Interlocked.Increment(ref _currentOutstandingRequests);
                OffloadStartNextRequest();

                await NegotiateWebSocket(socket);
            }
        }

        private void FinishSocket(Socket client)
        {
            try { client.Dispose(); }
            catch { }
        }
        private async Task NegotiateWebSocket(Socket client)
        {
            WebSocketNegotiationResult result;
            try
            {
                Stream stream = new NetworkStream(client, FileAccess.ReadWrite, true);
                foreach (var conExt in _extensions)
                {
                    stream = await conExt.ExtendConnectionAsync(stream);
                }
                var handshake = await _handShaker.HandshakeAsync(stream);

                if (handshake.IsValid)
                    result = new WebSocketNegotiationResult(handshake.Factory.CreateWebSocket(stream, _options, (IPEndPoint)client.LocalEndPoint, (IPEndPoint)client.RemoteEndPoint, handshake.Request, handshake.Response, handshake.NegotiatedMessageExtensions));
                else
                    result = new WebSocketNegotiationResult(handshake.Error);
            }
            catch (Exception ex)
            {
                FinishSocket(client);
                result = new WebSocketNegotiationResult(ExceptionDispatchInfo.Capture(ex));
            }
            try
            {
                await _negotiations.SendAsync(result, _cancel.Token).ConfigureAwait(false);
            }
            finally
            {
                Interlocked.Decrement(ref _currentOutstandingRequests);
            }
        }

        public void Queue(Socket socket)
        {
            if (!_sockets.Post(socket))
                FinishSocket(socket);
        }

        public Task<WebSocketNegotiationResult> DequeueAsync(CancellationToken cancel)
        {
            return _negotiations.ReceiveAsync(cancel);
        }

        private void Dispose(Boolean disposing)
        {
            _cancel.Cancel();
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            Dispose(true);
        }

        ~HttpNegotiationQueue()
        {
            Dispose(false);
        }
    }
}
