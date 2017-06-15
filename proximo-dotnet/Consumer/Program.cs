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
        private static IConsumerClient client;

        public static async Task MessageHandler((string, string) message, CancellationToken cancellationToken)
        {  
            await client.AcknowledgeMessage(message.Item1);
        }


        public static void Main(string[] args)
        {
            var channel = new Grpc.Core.Channel("localhost:6868", ChannelCredentials.Insecure);
            client = new ConsumerClient(new Proximo.MessageSource.MessageSourceClient(channel), "producer1", "int_test_topic");

            Func<(string, string), CancellationToken, Task> messageHandlerAction = MessageHandler;

            try
            {
                // Consume messages.
                Console.WriteLine("*** Start consuming messages");
                client.ConsumeMessages(messageHandlerAction, new CancellationToken()).Wait();
            }
            catch (Exception e)
            {
                Console.WriteLine($"RPC failed. {e.GetBaseException().Message}");
            }

            channel.ShutdownAsync().Wait();
            Console.WriteLine("*** Consumer stopped");
        }
    }
}

