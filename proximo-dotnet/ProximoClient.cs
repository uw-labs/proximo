using Google.Protobuf;
using Grpc.Core;
using Proximo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace proximo_dotnet
{
    /// <summary>
    /// Client code that consumes messages from the server.
    /// </summary>
    public class ConsumerClient : IConsumerClient
    {
        readonly MessageSource.MessageSourceClient _client;
        readonly string _clientId;
        readonly string _topic;

        public ConsumerClient(MessageSource.MessageSourceClient messageSourceClient, string clientId, string topic)
        {
            _client = messageSourceClient;
            _clientId = clientId;
            _topic = topic;
        }

        /// <summary>
        /// Consuming messages
        /// </summary>
        public async Task ConsumeMessages(Queue<(string, string)> messagesQueue, CancellationToken cancellationToken)
        {
            var confirmations = new Queue<Message>();
            try
            {
                using (var call = _client.Consume())
                {
                    Action<Message> consumerRequestAction = (async (Message confMsg) =>
                    {
                        var cr = new ConsumerRequest
                        {
                            Confirmation = new Confirmation { MsgID = confMsg.Id }
                        };

                        try
                        {
                            await call.RequestStream.WriteAsync(cr);
                        }
                        catch (Exception e)
                        {
                            throw e;
                        }
                    });

                    var responseReaderTask = Task.Run(() =>
                    {
                        //Reading messages
                        try
                        {
                            while (call.ResponseStream.MoveNext(cancellationToken).Result)
                            {
                                var confirm = call.ResponseStream.Current;
                                confirmations.Enqueue(confirm);
                                var data = confirm.Data.ToString(Encoding.UTF8);

                                //add to local queue
                                messagesQueue.Enqueue((confirm.Id, data));

                                var consumerRequestTask = new Task(r => consumerRequestAction(confirm), cancellationToken);
                                consumerRequestTask.RunSynchronously();
                            }
                        }
                        catch (Exception e)
                        {
                            throw e;
                        }
                    });


                    var scr = new ConsumerRequest
                    {
                        StartRequest = new StartConsumeRequest
                        {
                            Consumer = _clientId,
                            Topic = _topic
                        }
                    };

                    await call.RequestStream.WriteAsync(scr);
                    await responseReaderTask;
                }
            }
            catch (RpcException e)
            {
                throw e;
            }
        }
    }


    /// <summary>
    /// Client publisher that makes gRPC calls to the server.
    /// </summary>
    public class PublisherClient : IPublisherClient
    {
        readonly MessageSink.MessageSinkClient _client;
        readonly string _clientId;
        readonly string _topic;

        public PublisherClient(MessageSink.MessageSinkClient messageSinkClient, string clientId, string topic)
        {
            _client = messageSinkClient;
            _clientId = clientId;
            _topic = topic;
        }

        /// <summary>
        /// Bi-directional publishing of messages 
        /// </summary>
        public async Task PublishMessages(List<(string, string)> messagesList, Queue<string> receiveQueue)
        {
            try
            {
                var requests = messagesList.Select(m=> new Proximo.Message
                    {
                        Id = m.Item1,
                        Data = ByteString.CopyFromUtf8(m.Item2)
                });

                using (var call = _client.Publish(new CallOptions { }))
                {
                    var responseReaderTask = Task.Run(() =>
                    {
                        try
                        {
                            while (call.ResponseStream.MoveNext().Result)
                            {
                                var confirm = call.ResponseStream.Current;
                                receiveQueue.Enqueue(confirm.MsgID);
                            }
                        }
                        catch (Exception e)
                        {
                            throw e;
                        }
                       
                    });

                    var spr = new PublisherRequest
                    {
                        StartRequest = new StartPublishRequest { Topic = _topic}
                    };
                    await call.RequestStream.WriteAsync(spr);

                    foreach (Message request in requests)
                    {
                        var pr = new PublisherRequest
                        {
                            Msg = request
                        };

                        try
                        {
                            await call.RequestStream.WriteAsync(pr);
                        }
                        catch (Exception e)
                        {
                            throw e;
                        }
                    }

                    await call.RequestStream.CompleteAsync();
                    await responseReaderTask;
                }
            }
            catch (RpcException e)
            {
                throw e;
            }
        }
    }
}
