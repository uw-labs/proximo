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
        /// <param name="messagesQueue">The in-memory queue.</param>
        /// /// <param name="cancellationToken">The cancellation token.</param>
        Task ConsumeMessages(Queue<(string, string)> messagesQueue, CancellationToken cancellationToken);
    }
}