using System.Collections.Generic;
using System.Threading.Tasks;

namespace proximo_dotnet
{
    public interface IPublisherClient
    {
        /// <summary>
        /// Publish messages to proximo server and adds the confirmation ids to an in-memory queue
        /// </summary>
        /// <param name="receiveQueue">The in-memory queue.</param>
        Task PublishMessages(List<(string, string)> messagesList, Queue<string> receiveQueue);
    }
}