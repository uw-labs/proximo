using System;
using Grpc.Core;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using proximo_dotnet;

namespace proximo_consumer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var channel = new Grpc.Core.Channel("127.0.0.1:6868", ChannelCredentials.Insecure);
            IConsumerClient client = new ConsumerClient(new Proximo.MessageSource.MessageSourceClient(channel), "dotnetc-client", "new-topic");
            var messagesQ = new List<(string, string, double)>();

            Action useMessagesAction = (() =>
            {
                for (;;)
                {
                    Thread.Sleep(500);
                    if (messagesQ.Count > 0)
                    {
                        var msg = messagesQ;
                        Console.WriteLine($"Got {messagesQ.Count} messages.'");
                    }
                }
            });
            var startTask = new Task(r => useMessagesAction(), new CancellationToken());
            startTask.Start();

            try
            {
                // Consume messages.
                Console.WriteLine("*** Start consuming messages");
                client.ConsumeMessages(messagesQ, new CancellationToken()).Wait();
            }
            catch (Exception e)
            {
                Console.WriteLine($"RPC failed. {e.GetBaseException().Message}");
            }

            channel.ShutdownAsync().Wait();

            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }
    }
}

