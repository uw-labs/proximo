using System;
using Grpc.Core;
using Proximo;
using System.Text;
using Google.Protobuf;
using Grpc.Core.Utils;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using proximo_dotnet;

namespace proximo_producer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var channel = new Grpc.Core.Channel("127.0.0.1:6868", ChannelCredentials.Insecure);
            IPublisherClient client = new PublisherClient(new Proximo.MessageSink.MessageSinkClient(channel), "dotnetp-client", "new-topic");

            var messagesL = new List<(string, string)>() {
                ("1", "first message"),
                ("2", "second message")
            };
            var receiveQ = new Queue<string>();

            Action receiveAction = (() =>
            {
                for (;;)
                {
                    Thread.Sleep(500);
                    if (receiveQ.Count > 0)
                    {
                        var msg = receiveQ.Dequeue();
                        Console.WriteLine($"Message with id '{msg}' added to the queue");
                    }
                }
            });
            var startTask = new Task(r => receiveAction(), new CancellationToken());
            startTask.Start();

            try
            {
                // Send messages.
                Console.WriteLine("*** Start publishing the messages");

                client.PublishMessages(messagesL, receiveQ).Wait();
            }
            catch (Exception e)
            {
                Console.WriteLine($"RPC failed. {e.GetBaseException().Message}");
            }

            channel.ShutdownAsync().Wait();
            Console.WriteLine("*** Finished");
            Console.ReadKey();
        }

    }
}
