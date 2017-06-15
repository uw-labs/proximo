using Proximo;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace proximo_dotnet
{
    public interface IConsumerClient
    {
        /// <summary>
        /// Consume messages from proximo server and adds them to an in-memory queue
        /// </summary>
        /// <param name="messagesQueue">The in-memory queue. (id, message, time spent)</param>
        /// /// <param name="cancellationToken">The cancellation token.</param>
        Task ConsumeMessages(Func<(string, string), CancellationToken, Task> consumeHandler, CancellationToken cancellationToken);
        Task<bool> AcknowledgeMessage(string messageId);
    }
}