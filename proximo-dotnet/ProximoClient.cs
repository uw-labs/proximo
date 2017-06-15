using Google.Protobuf;
using Grpc.Core;
using Proximo;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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

        public ConsumerClient(MessageSource.MessageSourceClient messageSourceClient, string clientId)
        {
            _client = messageSourceClient;
            _clientId = clientId;

            Console.WriteLine($"Consumer {_clientId} connected");
        }

        /// <summary>
        /// Consume messages from proximo server and adds them to an in-memory queue
        /// </summary>
        /// <param name="messagesQueue">The in-memory queue. (id, message, time spent)</param>
        /// /// <param name="cancellationToken">The cancellation token.</param>
        public async Task ConsumeMessages(Func<(string, string), CancellationToken, Task> consumeHandler, CancellationToken cancellationToken, string topic)
        {
            try
            {
                using (var call = _client.Consume())
                {
                    //Stopwatch sw = null;

                    var responseReaderTask = Task.Run(async () =>
                    {
                        //Reading messages
                        try
                        {
                            while (call.ResponseStream.MoveNext(cancellationToken).Result)
                            {
                                //sw = Stopwatch.StartNew();

                                var confirm = call.ResponseStream.Current;
                                var data = confirm.Data.ToString(Encoding.UTF8);

                                await consumeHandler((confirm.Id, data), cancellationToken);

                               Thread.Sleep(10);

                                var cr = new ConsumerRequest
                                {
                                    Confirmation = new Confirmation { MsgID = confirm.Id }
                                };

                                try
                                {
                                    call.RequestStream.WriteAsync(cr).Wait();
                                }
                                catch (Exception e)
                                {
                                    throw e;
                                }
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
                            Topic = topic
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

        
        public async Task<bool> AcknowledgeMessage(string messageId)
        {
            try
            {
                using (var call = _client.Consume())
                {
                    //Confirmm message was received.
                    var cr = new ConsumerRequest
                    {
                        Confirmation = new Confirmation { MsgID = messageId }
                    };

                    try
                    {
                        await call.RequestStream.WriteAsync(cr);
                    }
                    catch (Exception e)
                    {
                        throw e;
                    }
                }
            }
            catch (Exception e)
            {
                throw e;
            }

            return true;
        }

    }



    /// <summary>
    /// Client publisher that makes gRPC calls to the server.
    /// </summary>
    public class PublisherClient : IPublisherClient
    {
        readonly MessageSink.MessageSinkClient _client;
        readonly string _clientId;

        public PublisherClient(MessageSink.MessageSinkClient messageSinkClient, string clientId)
        {
            _client = messageSinkClient;
            _clientId = clientId;

            Console.WriteLine($"Publisher {_clientId} connected");
        }

        /// <summary>
        /// Publish messages to proximo server and adds the confirmation ids to an in-memory queue
        /// </summary>
        /// <param name="messagesList">A list of string messages</param>
        /// <param name="receiveQueue">The in-memory queue.</param>
        public async Task<string> PublishMessages((string, string) message, string topic)
        {
            (string, byte[]) converted = (message.Item1, Encoding.UTF8.GetBytes(message.Item2));

            return await PublishMessages(converted, topic);
        }

        /// <summary>
        /// Publish messages to proximo server and adds the confirmation ids to an in-memory queue
        /// </summary>
        /// <param name="messagesList">A list of byte[] messages</param>
        public async Task<string> PublishMessages((string, byte[]) message, string topic)
        {
            string response = null;
            try
            {
                var request = new Proximo.Message
                {
                    Id = message.Item1,
                    Data = ByteString.CopyFrom(message.Item2)
                };

                using (var call = _client.Publish(new CallOptions { }))
                {
                    var responseReaderTask = Task.Run(() =>
                    {
                        try
                        {
                            while (call.ResponseStream.MoveNext().Result)
                            {
                                var confirm = call.ResponseStream.Current;

                                response = confirm.MsgID;
                                break;
                            }
                        }
                        catch (Exception e)
                        {
                            throw e;
                        }

                    });

                    var spr = new PublisherRequest
                    {
                        StartRequest = new StartPublishRequest { Topic = topic }
                    };
                    await call.RequestStream.WriteAsync(spr);


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

                    await call.RequestStream.CompleteAsync();
                    await responseReaderTask;
                }
            }
            catch (RpcException e)
            {
                throw e;
            }

            return response;
        }

    }
}
